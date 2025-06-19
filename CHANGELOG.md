# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release of Gosura Filter Inspector
- Extensible hook system with `FilterHook` interface
- SQL Parse Hook implementation
- PostgreSQL-specific Parse Hook
- Basic usage examples and developer documentation
- Comprehensive test suite, including PGX integration tests

### Features
- **Comparison Operators**: `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`
- **Null Checks**: `_is_null`
- **IN Clauses**: `_in`, `_nin`
- **Pattern Matching**: `_like`, `_nlike`, `_ilike`, `_nilike`
- **Regex Support**: `_regex`, `_nregex`, `_iregex`, `_niregex`, `_similar`, `_nsimilar`
- **JSONB Operators (PostgreSQL)**: `_contains`, `_contained_in`, `_has_key`, `_has_keys_any`, `_has_keys_all`
- **Logical Operators**: `_and`, `_or`, `_not`

### Technical
- Compatibility with Go 1.24+
- PostgreSQL integration using PGX
- GitHub Actions CI/CD workflow
- MIT License
- Project structure with examples and modular packages

## Version History

- **Unreleased** – Initial development release with full filter parsing, hook-based architecture, and PostgreSQL support