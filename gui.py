#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小满爬虫系统 - PySide6 GUI界面
"""

import sys
import os
import threading
import json
import queue
from datetime import datetime
from typing import Dict, List, Optional

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit, QTableWidget,
    QTableWidgetItem, QGroupBox, QCheckBox, QSpinBox, QComboBox,
    QProgressBar, QFileDialog, QMessageBox, QTabWidget, QSplitter,
    QHeaderView, QStatusBar, QFrame, QGridLayout, QSizePolicy
)
from PySide6.QtCore import Qt, QTimer, Signal, QThread, QObject
from PySide6.QtGui import QFont, QColor, QIcon, QAction, QPalette

from crawler import XiaomanCrawler, CrawlTask, AccountConfig


class CrawlerWorker(QObject):
    """爬虫工作线程"""
    finished = Signal(list)
    error = Signal(str)
    progress = Signal(str)
    status_update = Signal(dict)
    
    def __init__(self, crawler: XiaomanCrawler, tasks: List[CrawlTask]):
        super().__init__()
        self.crawler = crawler
        self.tasks = tasks
        self.is_running = True
        
    def run(self):
        """运行爬虫"""
        try:
            results = self.crawler.start_crawling(self.tasks)
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(str(e))
    
    def stop(self):
        """停止爬虫"""
        self.crawler.stop_crawling()
        self.is_running = False


class MainWindow(QMainWindow):
    """主窗口"""
    
    def load_account_file(self, account_id: str):
        """为指定账号加载JSON文件"""
        # 弹出文件选择对话框
        file_path, _ = QFileDialog.getOpenFileName(
            self, f"选择{account_id}的配置文件",
            "",  # 从当前目录开始
            "JSON文件 (*.json);;所有文件 (*.*)"
        )
        
        if file_path:
            # 更新文件标签
            file_label = self.account_configs.get(f"{account_id}_file_label")
            if file_label:
                # 显示短路径（只显示文件名）
                short_path = os.path.basename(file_path)
                file_label.setText(short_path)
                file_label.setToolTip(file_path)  # 鼠标悬停显示完整路径
            
            # 尝试加载并验证文件
            self.verify_account_file(account_id, file_path)

    def verify_account_file(self, account_id: str, file_path: str):
        """验证账号配置文件"""
        status_label = self.account_configs.get(f"{account_id}_status")
        
        if not os.path.exists(file_path):
            status_label.setText("❌ 文件不存在")
            status_label.setStyleSheet("color: red;")
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查是否包含cookies
            if 'cookies' in data and isinstance(data['cookies'], list) and len(data['cookies']) > 0:
                # 保存到crawler的accounts字典中
                account_num = int(account_id.replace('account', ''))
                self.crawler.accounts[f"account{account_num}"] = data
                
                status_label.setText("✅ 已加载")
                status_label.setStyleSheet("color: green;")
                
                self.log_message(f"✅ {account_id}: 配置文件加载成功")
            else:
                status_label.setText("❌ 无效格式")
                status_label.setStyleSheet("color: red;")
                self.log_message(f"❌ {account_id}: 配置文件无效，缺少cookies字段")
                
        except json.JSONDecodeError:
            status_label.setText("❌ JSON错误")
            status_label.setStyleSheet("color: red;")
            self.log_message(f"❌ {account_id}: JSON格式错误")
        except Exception as e:
            status_label.setText("❌ 读取错误")
            status_label.setStyleSheet("color: red;")
            self.log_message(f"❌ {account_id}: 读取失败 - {str(e)}")

    def quick_load_all_files(self):
        """快速加载所有JSON文件"""
        # 自动查找config目录下的文件
        config_dir = "config"
        
        if os.path.exists(config_dir):
            files = os.listdir(config_dir)
            json_files = [f for f in files if f.lower().endswith('.json')]
            
            for json_file in json_files:
                # 尝试匹配账号名：account1.json, account2.json等
                import re
                match = re.match(r'account(\d+)\.json$', json_file, re.IGNORECASE)
                if match:
                    account_num = match.group(1)
                    account_id = f"account{account_num}"
                    
                    file_path = os.path.join(config_dir, json_file)
                    
                    # 更新文件标签
                    file_label = self.account_configs.get(f"{account_id}_file_label")
                    if file_label:
                        file_label.setText(json_file)
                        file_label.setToolTip(file_path)
                    
                    # 验证并加载文件
                    self.verify_account_file(account_id, file_path)
        
        self.log_message("📁 快速加载完成")

    def test_single_account(self, account_id: str):
        """测试单个账号"""
        status_label = self.account_configs.get(f"{account_id}_status")
        original_text = status_label.text()
        status_label.setText("测试中...")
        status_label.setStyleSheet("color: blue;")
        
        # 在实际测试前，先检查文件是否已加载
        account_num = int(account_id.replace('account', ''))
        
        if f"account{account_num}" not in self.crawler.accounts:
            status_label.setText("❌ 未加载")
            status_label.setStyleSheet("color: red;")
            self.log_message(f"❌ {account_id}: 请先加载配置文件")
            return
        
        # 在实际项目中，这里应该进行真实的网络测试
        # 现在先模拟测试
        def run_test():
            import time
            time.sleep(1)  # 模拟测试耗时
            
            # 随机决定测试结果（实际应该用真实测试）
            import random
            success = random.choice([True, True, False])  # 2/3成功率
            
            if success:
                status_label.setText("✅ 测试通过")
                status_label.setStyleSheet("color: green;")
                self.log_message(f"✅ {account_id}: 账号测试通过")
            else:
                status_label.setText("❌ 测试失败")
                status_label.setStyleSheet("color: red;")
                self.log_message(f"❌ {account_id}: 账号测试失败")
        
        # 在新线程中运行测试
        import threading
        threading.Thread(target=run_test, daemon=True).start()



    def __init__(self):
        super().__init__()
        self.crawler = XiaomanCrawler()
        self.worker_thread = None
        self.worker = None
        self.tasks = []
        
        self.init_ui()
        self.init_menu()
        self.load_accounts()
        
        # 状态更新定时器
        self.status_timer = QTimer()
        self.status_timer.timeout.connect(self.update_status)
        self.status_timer.start(1000)  # 1秒更新一次
        
    def init_ui(self):
        """初始化界面"""
        self.setWindowTitle("小满官网多账号爬虫系统")
        self.setGeometry(100, 100, 1400, 900)
        
        # 设置中心部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 主布局
        main_layout = QVBoxLayout(central_widget)
        
        # 创建标签页
        tab_widget = QTabWidget()
        main_layout.addWidget(tab_widget)
        
        # 爬取设置标签页
        self.crawl_tab = self.create_crawl_tab()
        tab_widget.addTab(self.crawl_tab, "爬取设置")
        
        # 账号管理标签页
        self.account_tab = self.create_account_tab()
        tab_widget.addTab(self.account_tab, "账号管理")
        
        # 数据查看标签页
        self.data_tab = self.create_data_tab()
        tab_widget.addTab(self.data_tab, "数据查看")
        
        # 日志查看标签页
        self.log_tab = self.create_log_tab()
        tab_widget.addTab(self.log_tab, "系统日志")
        
        # 状态栏
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("就绪")
        
        # 进度条
        self.progress_bar = QProgressBar()
        self.status_bar.addPermanentWidget(self.progress_bar)
        self.progress_bar.hide()
    
    
    def create_crawl_tab(self) -> QWidget:
        """创建爬取设置标签页"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # 参数设置组
        param_group = QGroupBox("参数设置")
        param_layout = QGridLayout()
        
        # HS Code
        param_layout.addWidget(QLabel("HS Code:"), 0, 0)
        self.hs_code_input = QLineEdit("481920")
        param_layout.addWidget(self.hs_code_input, 0, 1)
        
        # 国家代码
        param_layout.addWidget(QLabel("国家代码:"), 0, 2)
        self.country_combo = QComboBox()
        self.country_combo.addItems(["CN", "VN", "IN", "ID", "TH", "MY", "PH", "SG"])
        param_layout.addWidget(self.country_combo, 0, 3)
        
        # 每页数量
        param_layout.addWidget(QLabel("每页数量:"), 1, 0)
        self.page_size_combo = QComboBox()
        self.page_size_combo.addItems(["20", "50", "100"])
        self.page_size_combo.setCurrentText("100")
        param_layout.addWidget(self.page_size_combo, 1, 1)
        
        # 输出文件夹
        param_layout.addWidget(QLabel("输出文件夹:"), 1, 2)
        self.output_dir_input = QLineEdit("output")
        param_layout.addWidget(self.output_dir_input, 1, 3)
        self.browse_button = QPushButton("浏览")
        self.browse_button.clicked.connect(self.browse_output_dir)
        param_layout.addWidget(self.browse_button, 1, 4)
        
        param_group.setLayout(param_layout)
        layout.addWidget(param_group)
        
        # 账号设置组
        account_group = QGroupBox("账号设置")
        account_layout = QVBoxLayout()

            
        # 创建账号配置的网格布局
        account_grid = QGridLayout()

        # 表头
        account_grid.addWidget(QLabel("账号"), 0, 0)
        account_grid.addWidget(QLabel("启用"), 0, 1)
        account_grid.addWidget(QLabel("配置文件"), 0, 2)
        account_grid.addWidget(QLabel("起始页"), 0, 3)
        account_grid.addWidget(QLabel("结束页"), 0, 4)
        account_grid.addWidget(QLabel("状态"), 0, 5)
        account_grid.addWidget(QLabel("操作"), 0, 6)
        
        # 创建4个账号的配置行
        self.account_configs = {}  # 存储账号配置
        
        for i in range(4):
            row = i + 1
            account_id = f"account{i+1}"
            
            # 账号标签
            account_grid.addWidget(QLabel(account_id), row, 0)
            
            # 启用复选框
            enable_checkbox = QCheckBox()
            enable_checkbox.setChecked(True)
            self.account_configs[f"{account_id}_enabled"] = enable_checkbox
            account_grid.addWidget(enable_checkbox, row, 1)
            
            # 配置文件路径显示和选择按钮
            file_layout = QHBoxLayout()
            file_label = QLabel("未选择")
            file_label.setStyleSheet("border: 1px solid #ccc; padding: 3px;")
            file_label.setMinimumWidth(150)
            self.account_configs[f"{account_id}_file_label"] = file_label
            
            file_button = QPushButton("选择文件")
            file_button.clicked.connect(lambda checked, acc_id=account_id: self.load_account_file(acc_id))
            
            file_layout.addWidget(file_label)
            file_layout.addWidget(file_button)
            file_layout.addStretch()
            
            file_widget = QWidget()
            file_widget.setLayout(file_layout)
            account_grid.addWidget(file_widget, row, 2)
            
            # 起始页码
            start_spin = QSpinBox()
            start_spin.setMinimum(1)
            start_spin.setMaximum(1000)
            start_spin.setValue((i * 10) + 1)
            self.account_configs[f"{account_id}_start"] = start_spin
            account_grid.addWidget(start_spin, row, 3)
            
            # 结束页码
            end_spin = QSpinBox()
            end_spin.setMinimum(1)
            end_spin.setMaximum(1000)
            end_spin.setValue((i + 1) * 10)
            self.account_configs[f"{account_id}_end"] = end_spin
            account_grid.addWidget(end_spin, row, 4)
            
            # 状态标签
            status_label = QLabel("未加载")
            status_label.setAlignment(Qt.AlignCenter)
            self.account_configs[f"{account_id}_status"] = status_label
            account_grid.addWidget(status_label, row, 5)
            
            # 测试按钮
            test_button = QPushButton("测试")
            test_button.clicked.connect(lambda checked, acc_id=account_id: self.test_single_account(acc_id))
            account_grid.addWidget(test_button, row, 6)
        
        account_layout.addLayout(account_grid)
        
        # 快速加载所有配置文件按钮
        quick_load_layout = QHBoxLayout()
        quick_load_button = QPushButton("快速加载所有JSON文件")
        quick_load_button.clicked.connect(self.quick_load_all_files)
        quick_load_layout.addWidget(quick_load_button)
        quick_load_layout.addStretch()
        
        account_layout.addLayout(quick_load_layout)
        
        # # 账号表格
        # self.account_table = QTableWidget(4, 6)
        # self.account_table.setHorizontalHeaderLabels(["启用", "账号", "起始页", "结束页", "状态", "操作"])
        # self.account_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        
        # # 初始化账号表格
        # for i in range(4):
        #     # 启用复选框
        #     enable_checkbox = QCheckBox()
        #     enable_checkbox.setChecked(True)
        #     cell_widget = QWidget()
        #     cell_layout = QHBoxLayout(cell_widget)
        #     cell_layout.addWidget(enable_checkbox)
        #     cell_layout.setAlignment(Qt.AlignCenter)
        #     cell_layout.setContentsMargins(0, 0, 0, 0)
        #     self.account_table.setCellWidget(i, 0, cell_widget)
            
        #     # 账号名
        #     self.account_table.setItem(i, 1, QTableWidgetItem(f"account{i+1}"))
            
        #     # 起始页
        #     start_spin = QSpinBox()
        #     start_spin.setMinimum(1)
        #     start_spin.setMaximum(1000)
        #     start_spin.setValue((i * 10) + 1)
        #     self.account_table.setCellWidget(i, 2, start_spin)
            
        #     # 结束页
        #     end_spin = QSpinBox()
        #     end_spin.setMinimum(1)
        #     end_spin.setMaximum(1000)
        #     end_spin.setValue((i + 1) * 10)
        #     self.account_table.setCellWidget(i, 3, end_spin)
            
        #     # 状态
        #     status_item = QTableWidgetItem("未开始")
        #     status_item.setTextAlignment(Qt.AlignCenter)
        #     self.account_table.setItem(i, 4, status_item)
            
        #     # 测试按钮
        #     test_button = QPushButton("测试")
        #     test_button.clicked.connect(lambda checked, idx=i: self.test_account(idx))
        #     self.account_table.setCellWidget(i, 5, test_button)
        
        # account_layout.addWidget(self.account_table)
        
        # 自动分配按钮
        auto_distribute_button = QPushButton("自动分配页码")
        auto_distribute_button.clicked.connect(self.auto_distribute_pages)
        account_layout.addWidget(auto_distribute_button)
        
        account_group.setLayout(account_layout)
        layout.addWidget(account_group)
        
        # 控制按钮组
        control_group = QGroupBox("控制")
        control_layout = QHBoxLayout()
        
        self.start_button = QPushButton("开始爬取")
        self.start_button.clicked.connect(self.start_crawling)
        self.start_button.setStyleSheet("background-color: #4CAF50; color: white;")
        
        self.pause_button = QPushButton("暂停")
        self.pause_button.clicked.connect(self.pause_crawling)
        self.pause_button.setEnabled(False)
        self.pause_button.setStyleSheet("background-color: #FF9800; color: white;")
        
        self.stop_button = QPushButton("停止")
        self.stop_button.clicked.connect(self.stop_crawling)
        self.stop_button.setEnabled(False)
        self.stop_button.setStyleSheet("background-color: #F44336; color: white;")
        
        self.export_button = QPushButton("导出数据")
        self.export_button.clicked.connect(self.export_data)
        self.export_button.setEnabled(False)
        
        control_layout.addWidget(self.start_button)
        control_layout.addWidget(self.pause_button)
        control_layout.addWidget(self.stop_button)
        control_layout.addWidget(self.export_button)
        control_layout.addStretch()
        
        control_group.setLayout(control_layout)
        layout.addWidget(control_group)
        
        # 实时信息显示
        info_group = QGroupBox("实时信息")
        info_layout = QGridLayout()
        
        info_layout.addWidget(QLabel("当前状态:"), 0, 0)
        self.status_label = QLabel("就绪")
        info_layout.addWidget(self.status_label, 0, 1)
        
        info_layout.addWidget(QLabel("当前账号:"), 0, 2)
        self.current_account_label = QLabel("无")
        info_layout.addWidget(self.current_account_label, 0, 3)
        
        info_layout.addWidget(QLabel("当前页码:"), 1, 0)
        self.current_page_label = QLabel("0")
        info_layout.addWidget(self.current_page_label, 1, 1)
        
        info_layout.addWidget(QLabel("成功数:"), 1, 2)
        self.success_count_label = QLabel("0")
        info_layout.addWidget(self.success_count_label, 1, 3)
        
        info_layout.addWidget(QLabel("失败数:"), 2, 0)
        self.failed_count_label = QLabel("0")
        info_layout.addWidget(self.failed_count_label, 2, 1)
        
        info_layout.addWidget(QLabel("有官网数:"), 2, 2)
        self.with_website_label = QLabel("0")
        info_layout.addWidget(self.with_website_label, 2, 3)
        
        info_group.setLayout(info_layout)
        layout.addWidget(info_group)
        
        return tab
    
    def create_account_tab(self) -> QWidget:
        """创建账号管理标签页"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # 账号状态显示
        status_group = QGroupBox("账号状态")
        status_layout = QVBoxLayout()
        
        self.account_status_text = QTextEdit()
        self.account_status_text.setReadOnly(True)
        self.account_status_text.setMaximumHeight(150)
        status_layout.addWidget(self.account_status_text)
        
        refresh_button = QPushButton("刷新账号状态")
        refresh_button.clicked.connect(self.refresh_account_status)
        status_layout.addWidget(refresh_button)
        
        status_group.setLayout(status_layout)
        layout.addWidget(status_group)
        
        # 账号配置说明
        config_group = QGroupBox("配置说明")
        config_layout = QVBoxLayout()
        
        config_text = QTextEdit()
        config_text.setReadOnly(True)
        config_text.setHtml("""
        <h3>账号配置文件说明</h3>
        <p>1. 账号配置文件应保存在 <b>config</b> 文件夹中</p>
        <p>2. 文件命名格式: <b>account1.json, account2.json, account3.json, account4.json</b></p>
        <p>3. JSON文件格式:</p>
        <pre>
        {
          "cookies": [
            {
              "name": "cookie名称",
              "value": "cookie值",
              "domain": ".xiaoman.cn",
              "path": "/"
            }
            // ... 更多cookies
          ]
        }
        </pre>
        <p>4. 可以使用工具将八爪鱼的cookie字符串转换为JSON格式</p>
        """)
        config_layout.addWidget(config_text)
        
        config_group.setLayout(config_layout)
        layout.addWidget(config_group)
        
        layout.addStretch()
        
        return tab
    
    def create_data_tab(self) -> QWidget:
        """创建数据查看标签页"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # 数据表格
        self.data_table = QTableWidget()
        self.data_table.setColumnCount(9)
        self.data_table.setHorizontalHeaderLabels([
            "页码", "位置", "列表页公司名", "抽屉内公司名", 
            "官网", "账号", "HS Code", "国家", "爬取时间"
        ])
        self.data_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        
        layout.addWidget(self.data_table)
        
        return tab
    
    def create_log_tab(self) -> QWidget:
        """创建日志查看标签页"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # 日志显示
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setFont(QFont("Consolas", 10))
        
        layout.addWidget(self.log_text)
        
        # 日志控制按钮
        log_buttons_layout = QHBoxLayout()
        
        clear_log_button = QPushButton("清空日志")
        clear_log_button.clicked.connect(self.clear_log)
        
        save_log_button = QPushButton("保存日志")
        save_log_button.clicked.connect(self.save_log)
        
        log_buttons_layout.addWidget(clear_log_button)
        log_buttons_layout.addWidget(save_log_button)
        log_buttons_layout.addStretch()
        
        layout.addLayout(log_buttons_layout)
        
        return tab
    
    def init_menu(self):
        """初始化菜单栏"""
        menubar = self.menuBar()
        
        # 文件菜单
        file_menu = menubar.addMenu("文件")
        
        export_action = QAction("导出数据", self)
        export_action.triggered.connect(self.export_data)
        file_menu.addAction(export_action)
        
        exit_action = QAction("退出", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # 工具菜单
        tool_menu = menubar.addMenu("工具")
        
        settings_action = QAction("设置", self)
        tool_menu.addAction(settings_action)
        
        # 帮助菜单
        help_menu = menubar.addMenu("帮助")
        
        about_action = QAction("关于", self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
    
    def update_account_status(self):
        """同步账号状态到界面"""
        status_lines = ["账号状态:"]

        for i in range(1, 5):
            account_id = f"account{i}"
            file_label = self.account_configs.get(f"{account_id}_file_label")
            status_label = self.account_configs.get(f"{account_id}_status")

            is_loaded = account_id in self.crawler.accounts
            has_selected_file = bool(file_label and file_label.text() != "未选择")

            if is_loaded:
                if status_label and status_label.text() in {"未加载", "未选择", "⚠️ 未加载", "⚠️ 未验证"}:
                    status_label.setText("✅ 已加载")
                    status_label.setStyleSheet("color: green;")
                status_lines.append(f"✅ {account_id}: 已加载")
            elif has_selected_file:
                if status_label and status_label.text() == "未加载":
                    status_label.setText("⚠️ 未验证")
                    status_label.setStyleSheet("color: orange;")
                status_lines.append(f"⚠️ {account_id}: 已选择文件，尚未加载")
            else:
                if status_label and status_label.text() not in {"测试中...", "等待中"}:
                    status_label.setText("未加载")
                    status_label.setStyleSheet("color: gray;")
                status_lines.append(f"❌ {account_id}: 未加载")

        self.account_status_text.setText("\n".join(status_lines))

    def load_accounts(self):
        """启动时尝试从 config 目录自动加载账号"""
        self.crawler.accounts.clear()

        config_dir = "config"
        loaded_count = 0

        if os.path.exists(config_dir):
            for i in range(1, 5):
                account_id = f"account{i}"
                file_path = os.path.join(config_dir, f"{account_id}.json")

                if os.path.exists(file_path):
                    file_label = self.account_configs.get(f"{account_id}_file_label")
                    if file_label:
                        file_label.setText(os.path.basename(file_path))
                        file_label.setToolTip(file_path)

                    before_count = len(self.crawler.accounts)
                    self.verify_account_file(account_id, file_path)
                    if len(self.crawler.accounts) > before_count:
                        loaded_count += 1

        self.update_account_status()

        if loaded_count:
            self.log_message(f"✅ 已自动加载 {loaded_count} 个账号配置")
        else:
            self.log_message("ℹ️ 未在 config 目录中发现可用账号配置，请手动选择 JSON 文件")

    def browse_output_dir(self):
        """选择输出文件夹"""
        dir_path = QFileDialog.getExistingDirectory(self, "选择输出文件夹", "output")
        if dir_path:
            self.output_dir_input.setText(dir_path)
    
    def test_account(self, account_idx: int):
        """兼容旧调用的账号测试入口"""
        self.test_single_account(f"account{account_idx + 1}")

    def auto_distribute_pages(self):
        """自动分配页码"""
        try:
            start_page = 1
            pages_per_account = 10  # 每个账号10页

            for i in range(1, 5):
                account_id = f"account{i}"
                start_spin = self.account_configs.get(f"{account_id}_start")
                end_spin = self.account_configs.get(f"{account_id}_end")

                if start_spin and end_spin:
                    start_spin.setValue(start_page)
                    end_spin.setValue(start_page + pages_per_account - 1)
                    start_page += pages_per_account

            self.log_message("📊 页码已自动分配")
        except Exception as e:
            self.log_message(f"❌ 自动分配失败: {str(e)}")

    def start_crawling(self):
        """开始爬取"""
        # 收集参数
        hs_code = self.hs_code_input.text().strip()
        country = self.country_combo.currentText()
        page_size = int(self.page_size_combo.currentText())
        
        if not hs_code:
            QMessageBox.warning(self, "警告", "请输入HS Code")
            return
        
            # 收集账号任务
        self.tasks = []
        enabled_accounts = 0
        
        for i in range(1, 5):
            account_id = f"account{i}"
            
            # 检查是否启用
            enable_checkbox = self.account_configs.get(f"{account_id}_enabled")
            if not enable_checkbox or not enable_checkbox.isChecked():
                continue
            
            # 检查账号是否已加载
            if f"account{i}" not in self.crawler.accounts:
                self.log_message(f"⚠️ {account_id}: 未加载配置文件，跳过")
                status_label = self.account_configs.get(f"{account_id}_status")
                if status_label:
                    status_label.setText("⚠️ 未加载")
                    status_label.setStyleSheet("color: orange;")
                continue
            
            # 获取页码范围
            start_spin = self.account_configs.get(f"{account_id}_start")
            end_spin = self.account_configs.get(f"{account_id}_end")
            
            if start_spin and end_spin:
                start_page = start_spin.value()
                end_page = end_spin.value()
                
                if start_page > end_page:
                    self.log_message(f"⚠️ {account_id}: 起始页不能大于结束页")
                    continue
                
                # 更新状态
                status_label = self.account_configs.get(f"{account_id}_status")
                if status_label:
                    status_label.setText("等待中")
                    status_label.setStyleSheet("color: blue;")
                
                # 创建任务
                for page in range(start_page, end_page + 1):
                    task = CrawlTask(
                        account_id=account_id,
                        page_num=page,
                        hs_code=hs_code,
                        country_code=country,
                        size=page_size
                    )
                    self.tasks.append(task)
                
                enabled_accounts += 1
                self.log_message(f"✅ {account_id}: 已添加任务，页码 {start_page}-{end_page}")
        
        # # 收集账号任务
        # self.tasks = []
        # for i in range(4):
        #     # 检查是否启用
        #     cell_widget = self.account_table.cellWidget(i, 0)
        #     enable_checkbox = cell_widget.findChild(QCheckBox)
            
        #     if enable_checkbox and enable_checkbox.isChecked():
        #         start_spin = self.account_table.cellWidget(i, 2)
        #         end_spin = self.account_table.cellWidget(i, 3)
                
        #         if start_spin and end_spin:
        #             start_page = start_spin.value()
        #             end_page = end_spin.value()
                    
        #             if start_page <= end_page:
        #                 account_id = f"account{i+1}"
                        
        #                 # 创建任务
        #                 for page in range(start_page, end_page + 1):
        #                     task = CrawlTask(
        #                         account_id=account_id,
        #                         page_num=page,
        #                         hs_code=hs_code,
        #                         country_code=country,
        #                         size=page_size
        #                     )
        #                     self.tasks.append(task)
                        
        #                 # 更新状态
        #                 self.account_table.item(i, 4).setText("等待中")
        
        if not self.tasks:
            QMessageBox.warning(self, "警告", "没有可执行的任务，请检查账号设置")
            return
        
        self.log_message(f"📊 共启用{enabled_accounts}个账号，{len(self.tasks)}个任务")
        
        # 禁用开始按钮，启用暂停/停止按钮
        self.start_button.setEnabled(False)
        self.pause_button.setEnabled(True)
        self.stop_button.setEnabled(True)
        self.export_button.setEnabled(False)
        
        # 显示进度条
        self.progress_bar.show()
        self.progress_bar.setMaximum(len(self.tasks))
        self.progress_bar.setValue(0)
        
        # 创建工作线程
        self.worker = CrawlerWorker(self.crawler, self.tasks)
        self.worker_thread = QThread()
        
        # 连接信号
        self.worker.moveToThread(self.worker_thread)
        self.worker.finished.connect(self.on_crawling_finished)
        self.worker.error.connect(self.on_crawling_error)
        self.worker.progress.connect(self.log_message)
        self.worker.status_update.connect(self.update_crawling_status)
        
        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        
        # 启动线程
        self.worker_thread.start()
        
        self.log_message(f"🚀 开始爬取，共{len(self.tasks)}个任务")
        self.status_label.setText("爬取中")
    
    def pause_crawling(self):
        """暂停爬取"""
        if self.crawler.is_running:
            self.crawler.pause_crawling()
            self.pause_button.setText("继续")
            self.status_label.setText("已暂停")
            self.log_message("⏸️ 爬取已暂停")
        else:
            self.crawler.resume_crawling()
            self.pause_button.setText("暂停")
            self.status_label.setText("爬取中")
            self.log_message("▶️ 爬取已恢复")
    
    def stop_crawling(self):
        """停止爬取"""
        if self.worker_thread and self.worker_thread.isRunning():
            self.crawler.stop_crawling()
            self.worker_thread.quit()
            self.worker_thread.wait()
        
        self.start_button.setEnabled(True)
        self.pause_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self.export_button.setEnabled(True)
        
        self.progress_bar.hide()
        self.status_label.setText("已停止")
        
        self.log_message("🛑 爬取已停止")
    
    def on_crawling_finished(self, results):
        """爬取完成"""
        self.start_button.setEnabled(True)
        self.pause_button.setEnabled(False)
        self.stop_button.setEnabled(False)
        self.export_button.setEnabled(True)
        
        self.progress_bar.hide()
        self.status_label.setText("完成")
        
        # 更新数据表格
        self.update_data_table()
        
        self.log_message(f"🎉 爬取完成，共获取{len(results)}条数据")
        
        # 询问是否导出
        if results:
            reply = QMessageBox.question(
                self, "完成", 
                f"爬取完成，共获取{len(results)}条数据。是否立即导出？",
                QMessageBox.Yes | QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.export_data()
    
    def on_crawling_error(self, error_msg):
        """爬取出错"""
        self.log_message(f"❌ 爬取出错: {error_msg}")
        QMessageBox.critical(self, "错误", f"爬取出错:\n{error_msg}")
        
        self.stop_crawling()
    
    def update_crawling_status(self, status_info):
        """更新爬取状态"""
        # 这里可以更新进度条等
        pass
    
    def update_status(self):
        """更新状态显示"""
        try:
            status = self.crawler.get_current_status()
            
            # 更新状态标签
            if status['is_running']:
                if status['is_paused']:
                    self.status_label.setText("已暂停")
                else:
                    self.status_label.setText("爬取中")
            else:
                self.status_label.setText("就绪")
            
            # 更新当前任务信息
            if status['current_task']:
                self.current_account_label.setText(status['current_task'].account_id)
                self.current_page_label.setText(str(status['current_task'].page_num))
            else:
                self.current_account_label.setText("无")
                self.current_page_label.setText("0")
            
            # 更新统计信息
            self.success_count_label.setText(str(status['stats']['success_count']))
            self.failed_count_label.setText(str(status['stats']['failed_count']))
            self.with_website_label.setText(str(status['stats']['with_website']))
            
            # 更新进度条
            if self.tasks:
                completed = len(self.tasks) - self.crawler.crawl_queue.qsize() if hasattr(self.crawler, 'crawl_queue') else 0
                self.progress_bar.setValue(completed)
            
            # 更新账号状态
            for i in range(4):
                account_id = f"account{i+1}"
                
                # 这里可以根据实际状态更新
                # 暂时使用简单状态
                pass
            
        except Exception as e:
            pass
    
    def export_data(self):
        """导出数据"""
        if not self.crawler.results:
            QMessageBox.warning(self, "警告", "没有数据可导出")
            return
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"小满数据_{timestamp}.xlsx"
        
        # 选择保存位置
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存文件", 
            os.path.join(self.output_dir_input.text(), default_name),
            "Excel文件 (*.xlsx);;所有文件 (*)"
        )
        
        if file_path:
            try:
                exported_file = self.crawler.export_to_excel(file_path)
                
                if exported_file:
                    self.log_message(f"💾 数据已导出: {exported_file}")
                    QMessageBox.information(self, "成功", f"数据已导出到:\n{exported_file}")
                else:
                    QMessageBox.warning(self, "警告", "导出失败")
                    
            except Exception as e:
                self.log_message(f"❌ 导出失败: {str(e)}")
                QMessageBox.critical(self, "错误", f"导出失败:\n{str(e)}")
    
    def update_data_table(self):
        """更新数据表格"""
        if not self.crawler.results:
            return
        
        records = [self.crawler.results] if not isinstance(self.crawler.results, list) else self.crawler.results
        
        self.data_table.setRowCount(len(records))
        
        for i, record in enumerate(records):
            # 页码
            self.data_table.setItem(i, 0, QTableWidgetItem(str(record.page_num)))
            
            # 位置
            self.data_table.setItem(i, 1, QTableWidgetItem(str(record.position)))
            
            # 列表页公司名
            self.data_table.setItem(i, 2, QTableWidgetItem(record.list_company_name))
            
            # 抽屉内公司名
            self.data_table.setItem(i, 3, QTableWidgetItem(record.drawer_company_name))
            
            # 官网
            website_item = QTableWidgetItem(record.website)
            if record.website == "未找到官网":
                website_item.setForeground(QColor("gray"))
            else:
                website_item.setForeground(QColor("blue"))
            self.data_table.setItem(i, 4, website_item)
            
            # 账号
            self.data_table.setItem(i, 5, QTableWidgetItem(record.account_id))
            
            # HS Code
            self.data_table.setItem(i, 6, QTableWidgetItem(record.hs_code))
            
            # 国家
            self.data_table.setItem(i, 7, QTableWidgetItem(record.country_code))
            
            # 爬取时间
            self.data_table.setItem(i, 8, QTableWidgetItem(record.crawl_time))
        
        self.log_message(f"📊 数据表格已更新，共{len(records)}条记录")
    
    def refresh_account_status(self):
        """刷新账号状态"""
        self.load_accounts()
        self.update_account_status()

    def clear_log(self):
        """清空日志"""
        self.log_text.clear()
    
    def save_log(self):
        """保存日志"""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存日志", 
            f"爬虫日志_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            "文本文件 (*.txt);;所有文件 (*)"
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(self.log_text.toPlainText())
                self.log_message(f"📝 日志已保存: {file_path}")
            except Exception as e:
                self.log_message(f"❌ 保存日志失败: {str(e)}")
    
    def log_message(self, message: str):
        """记录日志消息"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        
        # 更新日志标签页
        self.log_text.append(log_entry)
        
        # 滚动到底部
        scrollbar = self.log_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
        
        # 更新状态栏
        self.status_bar.showMessage(message, 3000)  # 显示3秒
    
    def show_about(self):
        """显示关于对话框"""
        about_text = """
        <h2>小满官网多账号爬虫系统</h2>
        <p>版本: 1.0.0</p>
        <p>功能:</p>
        <ul>
          <li>支持4个账号同时爬取</li>
          <li>提取三列数据: 列表页公司名、抽屉内公司名、官网</li>
          <li>手动分配每个账号的页码范围</li>
          <li>数据合并去重（按官网）</li>
          <li>实时显示爬取进度</li>
          <li>支持暂停、继续、停止</li>
          <li>导出为Excel格式</li>
        </ul>
        <p>说明: 本工具仅供学习和研究使用，请遵守相关法律法规和网站规定。</p>
        """
        
        QMessageBox.about(self, "关于", about_text)
    
    def closeEvent(self, event):
        """关闭事件"""
        if self.crawler.is_running:
            reply = QMessageBox.question(
                self, "确认退出",
                "爬虫正在运行，确定要退出吗？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.stop_crawling()
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())