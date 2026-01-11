# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/).

---

## [v0.2.0] – 2026-01-11

### ✨ Added
- CSV to SQL Server import tooling with one-command execution
- Support for both Windows Authentication and SQL Authentication
- Safe import behavior that skips execution when the target database already exists

### 🎛 UI Improvements
- Clearer regeneration intent in the Generate UI
- Improved handling and visibility of forced dimension regeneration

### 📄 Documentation
- Overhauled README with structured setup, generation, and SQL Server import instructions
- Added copy-paste–ready examples for both authentication modes
- Improved guidance around CSV output and SQL import workflow

### 🛠 Improvements
- More predictable and repeatable data generation workflows
- Better separation between generator logic and optional import tooling

---

## [v0.1.0] – 2026-01-10

### 🎉 Initial Release
- Synthetic data generator based on Contoso-style retail schema
- Configurable dimensions and sales fact generation
- Support for CSV, Parquet, and Delta Parquet outputs
- Power BI templates and sample reports
- Web UI for interactive data generation
