#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
小满官网多账号爬虫系统 - 主程序入口
"""

import sys
import os
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt
from PySide6.QtGui import QIcon
import qdarkstyle
from gui import MainWindow

def main():
    """主函数"""
    # 创建必要的目录
    os.makedirs("config", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    # 创建应用
    app = QApplication(sys.argv)
    
    # 设置样式
    app.setStyleSheet(qdarkstyle.load_stylesheet(qt_api='pyside6'))
    
    # 创建主窗口
    window = MainWindow()
    window.show()
    
    # 运行应用
    sys.exit(app.exec())

if __name__ == "__main__":
    main()