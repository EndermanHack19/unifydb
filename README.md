# 🗄️ UnifyDB

> **One library to rule them all databases.**

Unified Python interface for 15+ database systems with an optional web dashboard.

[![PyPI version](https://badge.fury.io/py/unifydb.svg)](https://badge.fury.io/py/unifydb)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## ✨ Features

- 🔌 **15+ Databases** - PostgreSQL, MySQL, MongoDB, Redis, SQLite, and more
- 🎯 **Unified API** - Same interface for all databases
- 🔄 **Auto-detection** - Connect using URI strings
- 📦 **Modular** - Install only what you need
- 🌐 **Web Dashboard** - Visual database management
- ⚡ **Async Support** - For high-performance applications
- 🛡️ **Type Hints** - Full typing support

## 📦 Installation

```bash
# Core only (no database drivers)
pip install unifydb

# With specific database
pip install unifydb[postgresql]
pip install unifydb[mysql]
pip install unifydb[mongodb]
pip install unifydb[redis]

# All databases
pip install unifydb[all]

# Web dashboard (separate from [all])
pip install unifydb[web]

# Everything
pip install unifydb[full]
