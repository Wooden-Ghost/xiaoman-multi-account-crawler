#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫核心功能 - 支持多账号、三列数据提取
"""

import json
import time
import pandas as pd
import random
import threading
import queue
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
import logging
from playwright.sync_api import sync_playwright, Page, BrowserContext

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_debug.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class CompanyRecord:
    """公司数据记录"""
    page_num: int
    position: int
    list_company_name: str
    drawer_company_name: str
    website: str
    account_id: str
    hs_code: str
    country_code: str
    crawl_time: str = field(default_factory=lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    status: str = "success"
    error_msg: str = ""

@dataclass
class AccountConfig:
    """账号配置"""
    account_id: str
    cookies_file: str
    start_page: int = 1
    end_page: int = 1
    is_enabled: bool = True

@dataclass
class CrawlTask:
    """爬取任务"""
    account_id: str
    page_num: int
    hs_code: str
    country_code: str
    size: int = 100

class XiaomanCrawler:
    """小满爬虫核心类"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = config_dir
        self.accounts: Dict[str, Dict] = {}
        self.crawl_queue = queue.Queue()
        self.results: List[CompanyRecord] = []
        self.is_running = False
        self.is_paused = False
        self.verification_required = False
        self.current_task = None
        self.stats = {
            'total_crawled': 0,
            'success_count': 0,
            'failed_count': 0,
            'with_website': 0,
            'without_website': 0
        }
        
        # 线程锁
        self.lock = threading.Lock()
        
        logger.info("爬虫引擎初始化完成")
    
    def load_accounts(self) -> bool:
        # """加载所有账号配置"""
        # self.accounts.clear()
        
        # for i in range(1, 5):
        #     file_path = os.path.join(self.config_dir, f"account{i}.json")
        #     if os.path.exists(file_path):
        #         try:
        #             with open(file_path, 'r', encoding='utf-8') as f:
        #                 account_data = json.load(f)
        #             self.accounts[f"account{i}"] = account_data
        #             logger.info(f"✅ 加载账号{i}: {file_path}")
        #         except Exception as e:
        #             logger.error(f"❌ 加载账号{i}失败: {e}")
        #     else:
        #         logger.warning(f"⚠️ 账号{i}配置文件不存在: {file_path}")
        
        return True
    
    def setup_browser_context(self, account_id: str) -> Optional[Tuple]:
        """为账号设置浏览器上下文"""
        if account_id not in self.accounts:
            logger.error(f"账号 {account_id} 未加载")
            return None
        
        try:
            p = sync_playwright().start()
            browser = p.chromium.launch(
                headless=False,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--start-maximized'
                ]
            )
            
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            # 添加cookies
            cookies = self.accounts[account_id].get('cookies', [])
            for cookie in cookies:
                clean_cookie = {k: v for k, v in cookie.items() 
                              if k not in ['sameSite', 'partitionKey', '_crHasCrossSiteAncestor']}
                context.add_cookies([clean_cookie])
            
            page = context.new_page()
            return p, browser, context, page
            
        except Exception as e:
            logger.error(f"设置浏览器失败: {e}")
            return None
    
    def construct_url(self, task: CrawlTask) -> str:
        """构建爬取URL"""
        base_url = "https://crm.xiaoman.cn/new_discovery/ciq-datum/list"
        params = {
            "data_type": "IMPDAT",
            "latest_months": "24",
            "exclude_logistics": "0",
            "has_contacts": "1",
            "hs_code": task.hs_code,
            "keyword_operator": "OR",
            "exporter_country_codes": task.country_code,
            "size": str(task.size),
            "company_type": "IMPORTER",
            "page": str(task.page_num)
        }
        
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}?{query_string}"
    
    def extract_company_data(self, page: Page, list_name: str, task: CrawlTask, position: int) -> Optional[CompanyRecord]:
        """提取公司三列数据"""
        try:
            # 等待抽屉加载
            page.wait_for_selector(".okki-drawer-body", timeout=10000)
            time.sleep(1)
            
            # 提取抽屉内公司名
            drawer_name = "未获取到"
            drawer_selectors = [
                '.okki-drawer-title div.ellipsis',
                '.okki-drawer-title .ellipsis.max-w-240px',
                '[class*="drawer-title"] .ellipsis',
                '.okki-drawer-header-title .ellipsis'
            ]
            
            for selector in drawer_selectors:
                try:
                    element = page.locator(selector).first
                    if element and element.is_visible():
                        drawer_name = element.text_content().strip()
                        break
                except:
                    continue
            
            # 提取官网
            website = "未找到官网"
            website_element = page.locator("span.normal-url").first
            if website_element and website_element.is_visible():
                website_text = website_element.text_content().strip()
                if website_text and ("http://" in website_text or "https://" in website_text):
                    website = website_text
            
            # 创建记录
            record = CompanyRecord(
                page_num=task.page_num,
                position=position,
                list_company_name=list_name,
                drawer_company_name=drawer_name,
                website=website,
                account_id=task.account_id,
                hs_code=task.hs_code,
                country_code=task.country_code
            )
            
            return record
            
        except Exception as e:
            logger.error(f"提取数据失败: {e}")
            return None
    
    def check_verification(self, page: Page) -> bool:
        """检查是否需要滑块验证"""
        try:
            verification_elements = [
                "text=访问频繁，请完成验证",
                "text=验证码",
                "text=滑块验证",
                ".geetest",
                ".slider-captcha"
            ]
            
            for selector in verification_elements:
                if page.locator(selector).count() > 0:
                    self.verification_required = True
                    logger.warning("⚠️ 检测到验证码/滑块验证")
                    return True
        except:
            pass
        return False
    
    def close_drawer(self, page: Page):
        """关闭抽屉"""
        try:
            # 尝试关闭按钮
            close_btn = page.locator(".close-btn").first
            if close_btn and close_btn.is_visible():
                close_btn.click()
            else:
                # 点击空白区域
                page.mouse.click(50, 50)
            
            page.wait_for_selector(".okki-drawer", state="hidden", timeout=3000)
            time.sleep(0.5)
        except:
            pass
    
    def crawl_page(self, task: CrawlTask) -> List[CompanyRecord]:
        """爬取单个页面"""
        page_records = []
        
        # 设置浏览器
        browser_data = self.setup_browser_context(task.account_id)
        if not browser_data:
            return page_records
        
        p, browser, context, page = browser_data
        
        try:
            # 访问页面
            url = self.construct_url(task)
            logger.info(f"{task.account_id} - 访问第{task.page_num}页: {url}")
            
            # 重试机制
            for retry in range(3):
                try:
                    page.goto(url, timeout=60000)
                    page.wait_for_load_state('domcontentloaded')
                    time.sleep(2)
                    break
                except Exception as e:
                    if retry < 2:
                        logger.warning(f"第{retry+1}次重试...")
                        time.sleep(3)
                    else:
                        raise
            
            # 等待公司列表加载
            page.wait_for_selector("a.click-hover-color", timeout=15000)
            time.sleep(1)
            
            # 获取公司列表
            company_elements = page.locator("a.click-hover-color")
            company_count = company_elements.count()
            
            logger.info(f"{task.account_id} - 第{task.page_num}页找到{company_count}个公司")
            
            # 处理每个公司
            for i in range(company_count):
                # 检查是否暂停
                while self.is_paused:
                    time.sleep(1)
                
                # 检查验证码
                if self.check_verification(page):
                    logger.warning(f"{task.account_id} - 需要手动处理验证码")
                    self.verification_required = True
                    break
                
                try:
                    # 重新获取元素
                    company_elements = page.locator("a.click-hover-color")
                    company_element = company_elements.nth(i)
                    
                    if not company_element.is_visible():
                        continue
                    
                    # 获取列表页公司名
                    list_name = company_element.text_content().strip()
                    
                    # 点击打开抽屉
                    company_element.click()
                    time.sleep(2)
                    
                    # 提取数据
                    record = self.extract_company_data(page, list_name, task, i+1)
                    
                    if record:
                        page_records.append(record)
                        
                        # 更新统计
                        with self.lock:
                            self.stats['total_crawled'] += 1
                            self.stats['success_count'] += 1
                            if record.website != "未找到官网":
                                self.stats['with_website'] += 1
                            else:
                                self.stats['without_website'] += 1
                    
                    # 关闭抽屉
                    self.close_drawer(page)
                    
                    # 控制速度
                    wait_time = random.uniform(10, 15)
                    time.sleep(wait_time)
                    
                except Exception as e:
                    logger.error(f"处理公司失败: {e}")
                    with self.lock:
                        self.stats['failed_count'] += 1
                    continue
            
            logger.info(f"{task.account_id} - 第{task.page_num}页完成，获取{len(page_records)}条记录")
            
        except Exception as e:
            logger.error(f"爬取第{task.page_num}页失败: {e}")
        finally:
            # 关闭浏览器
            try:
                browser.close()
                p.stop()
            except:
                pass
        
        return page_records
    
    def start_crawling(self, tasks: List[CrawlTask]):
        """开始爬取"""
        self.is_running = True
        self.is_paused = False
        self.verification_required = False
        
        # 清空结果
        self.results.clear()
        self.stats = {
            'total_crawled': 0,
            'success_count': 0,
            'failed_count': 0,
            'with_website': 0,
            'without_website': 0
        }
        
        # 创建任务队列
        self.crawl_queue = queue.Queue()
        for task in tasks:
            self.crawl_queue.put(task)
        
        # 创建工作线程
        num_workers = min(4, len(tasks))  # 最多4个线程
        threads = []
        
        for i in range(num_workers):
            thread = threading.Thread(target=self._worker, daemon=True)
            thread.start()
            threads.append(thread)
        
        logger.info(f"🚀 启动{num_workers}个工作线程，共{len(tasks)}个任务")
        
        # 等待所有任务完成
        self.crawl_queue.join()
        
        self.is_running = False
        logger.info("🎉 所有爬取任务完成")
        
        return self.results
    
    def _worker(self):
        """工作线程"""
        while self.is_running:
            try:
                task = self.crawl_queue.get(timeout=1)
                if task:
                    self.current_task = task
                    page_records = self.crawl_page(task)
                    
                    with self.lock:
                        self.results.extend(page_records)
                    
                    self.crawl_queue.task_done()
                    self.current_task = None
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"工作线程异常: {e}")
                try:
                    self.crawl_queue.task_done()
                except:
                    pass
    
    def pause_crawling(self):
        """暂停爬取"""
        self.is_paused = True
        logger.info("⏸️ 爬取已暂停")
    
    def resume_crawling(self):
        """恢复爬取"""
        self.is_paused = False
        logger.info("▶️ 爬取已恢复")
    
    def stop_crawling(self):
        """停止爬取"""
        self.is_running = False
        self.is_paused = False
        logger.info("🛑 爬取已停止")
    
    def get_current_status(self) -> Dict:
        """获取当前状态"""
        return {
            'is_running': self.is_running,
            'is_paused': self.is_paused,
            'verification_required': self.verification_required,
            'current_task': self.current_task,
            'stats': self.stats.copy(),
            'total_results': len(self.results)
        }
    
    def export_to_excel(self, filename: str):
        """导出数据到Excel"""
        if not self.results:
            logger.warning("没有数据可导出")
            return None
        
        # 转换为DataFrame
        records_dict = [asdict(record) for record in self.results]
        df = pd.DataFrame(records_dict)
        
        # 去重逻辑（按官网去重，空官网不去重）
        df_with_website = df[df['website'] != "未找到官网"]
        df_without_website = df[df['website'] == "未找到官网"]
        
        df_with_website_dedup = df_with_website.drop_duplicates(subset=['website'], keep='first')
        
        # 合并
        merged_df = pd.concat([df_with_website_dedup, df_without_website], ignore_index=True)
        
        # 排序
        merged_df = merged_df.sort_values(['page_num', 'position'])
        
        try:
            # 保存为Excel，多个sheet
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # 原始数据
                df.to_excel(writer, sheet_name='原始数据', index=False)
                # 合并去重数据
                merged_df.to_excel(writer, sheet_name='合并去重数据', index=False)
                # 统计信息
                stats_df = self._create_stats_dataframe(df, merged_df)
                stats_df.to_excel(writer, sheet_name='统计信息', index=False)
            
            logger.info(f"💾 数据已导出: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"导出数据失败: {e}")
            return None
    
    def _create_stats_dataframe(self, raw_df: pd.DataFrame, merged_df: pd.DataFrame) -> pd.DataFrame:
        """创建统计信息DataFrame"""
        stats = {
            '统计项': [
                '总爬取条数',
                '成功条数',
                '失败条数',
                '有官网条数',
                '无官网条数',
                '去重后总条数',
                '去重后有官网条数',
                '账号1爬取数',
                '账号2爬取数',
                '账号3爬取数',
                '账号4爬取数',
                '最早爬取时间',
                '最晚爬取时间'
            ],
            '数值': [
                len(raw_df),
                self.stats['success_count'],
                self.stats['failed_count'],
                self.stats['with_website'],
                self.stats['without_website'],
                len(merged_df),
                len(merged_df[merged_df['website'] != "未找到官网"]),
                len(raw_df[raw_df['account_id'] == 'account1']),
                len(raw_df[raw_df['account_id'] == 'account2']),
                len(raw_df[raw_df['account_id'] == 'account3']),
                len(raw_df[raw_df['account_id'] == 'account4']),
                raw_df['crawl_time'].min() if not raw_df.empty else 'N/A',
                raw_df['crawl_time'].max() if not raw_df.empty else 'N/A'
            ]
        }
        
        return pd.DataFrame(stats)