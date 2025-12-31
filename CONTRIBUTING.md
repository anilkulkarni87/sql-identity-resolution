# Contributing to SQL Identity Resolution

Thank you for considering contributing to SQL Identity Resolution! This guide will help you get started.

## Getting Started

### 1. Fork and Clone

```bash
git clone https://github.com/YOUR_USERNAME/sql-identity-resolution.git
cd sql-identity-resolution
```

### 2. Set Up Development Environment

```bash
# Install dependencies
pip install duckdb pyarrow

# Install dev dependencies
pip install flake8 pytest

# Verify setup
make test
```

### 3. Create a Branch

```bash
git checkout -b feature/your-feature-name
```

## Development Workflow

### Running Tests

```bash
# Run all tests
make test

# Run specific test
python tests/run_tests_duckdb.py
```

### Code Style

We use standard Python conventions:
- Max line length: 120 characters
- Use meaningful variable names
- Add docstrings to functions

```bash
# Lint your code
make lint
```

### Documentation

- Update docs if you change behavior
- Add docstrings to new functions
- Update README if adding features

```bash
# Preview docs locally
make docs
```

## Types of Contributions

### 🐛 Bug Reports

1. Check existing issues first
2. Include reproduction steps
3. Include platform/version info
4. Attach relevant logs

### ✨ Feature Requests

1. Describe the use case
2. Explain expected behavior
3. Consider backward compatibility

### 🔧 Pull Requests

1. Reference related issues
2. Include tests for new features
3. Update documentation
4. Keep changes focused

## Pull Request Process

1. **Create PR** with clear description
2. **Pass CI** - all tests must pass
3. **Review** - address feedback
4. **Merge** - maintainer will merge

### PR Checklist

- [ ] Tests pass locally (`make test`)
- [ ] Lint passes (`make lint`)
- [ ] Documentation updated (if needed)
- [ ] Commit messages are clear
- [ ] PR description explains changes

## Code Structure

```
sql-identity-resolution/
├── sql/
│   ├── duckdb/       # DuckDB implementation
│   ├── snowflake/    # Snowflake implementation
│   ├── bigquery/     # BigQuery implementation
│   └── databricks/   # Databricks implementation
├── tools/
│   ├── dashboard/    # Dashboard generator
│   ├── scale_test/   # Scale testing tools
│   └── ...
├── tests/            # Test suites
├── docs/             # Documentation
└── examples/         # Example configs and templates
```

## Platform-Specific Guidelines

### Adding a New Platform

1. Create directory under `sql/<platform>/`
2. Implement `00_ddl_all.sql` for schema
3. Implement runner script
4. Add tests
5. Add documentation

### Modifying Core Logic

When changing the matching/clustering algorithm:
1. Update all platform implementations
2. Ensure determinism is maintained
3. Add tests for edge cases
4. Document the change

## Questions?

- Open a [GitHub Issue](https://github.com/anilkulkarni87/sql-identity-resolution/issues)
- Check existing [documentation](https://anilkulkarni87.github.io/sql-identity-resolution/)

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
