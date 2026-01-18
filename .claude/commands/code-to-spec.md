---
description: Generate specification document from existing code
---

# Generate Specification from Code

Analyze code and generate comprehensive specification document.

## Arguments

`$ARGUMENTS`: File path, module path, class name, function name, `api`, or `architecture`.

## Instructions

1. **Read and Analyze**: Parse structure, extract docstrings/comments, identify public vs private APIs, map dependencies.

2. **Extract Interface Specs**:
   - **Classes**: Hierarchy, public attributes/types, methods with signatures, docstrings
   - **Functions**: Signature, parameters, return type, exceptions, side effects
   - **Modules**: Public exports, functions, classes, constants

3. **Document Behavior**: Method purpose, preconditions, postconditions, invariants, side effects.

4. **Map Dependencies**: External packages, internal modules, configuration, environment variables.

5. **Identify Constraints**: Input validation, state requirements, ordering, concurrency.

6. **Capture Data Structures**: Classes/attributes, TypedDicts, dataclasses, enums, configuration schemas.

7. **Generate Spec**: Consistent markdown, include code examples, note undocumented behavior, flag inconsistencies.

## Output Format

```markdown
## Specification: {name}

### Overview
{Brief description from docstring}

### Location
**File**: `{file_path}` | **Module**: `{module.path}` | **Lines**: {start}-{end}

### Public Interface

#### `ClassName`
{Class docstring}

**Inheritance**: `Parent` -> `ClassName`

**Attributes**:
| Name | Type | Description | Default |
| ---- | ---- | ----------- | ------- |

**Methods**:
| Method | Signature | Description |
| ------ | --------- | ----------- |

##### `method(arg: Type) -> ReturnType`
**Parameters**: `arg` (Type): Description
**Returns**: ReturnType - Description
**Raises**: `ValueError` - When {condition}

### Dependencies
| Package/Module | Purpose |
| -------------- | ------- |

### Configuration
| Parameter | Type | Default | Required |
| --------- | ---- | ------- | -------- |

### Environment Variables
| Variable | Purpose | Default |
| -------- | ------- | ------- |

### Behavior Specification
**Preconditions**: {conditions before calling}
**Postconditions**: {conditions after calling}
**Invariants**: {always true}
**Side Effects**: {external state changes}

### Error Handling
| Exception | Condition | Recovery |
| --------- | --------- | -------- |

### Examples
```python
from {project}.module import ClassName
result = ClassName(config).method(input)
```

### Notes
- {Implementation details, limitations, performance, thread safety}
```

## Example Usage

```
/code-to-spec tpch/
/code-to-spec TPCHBenchmark
/code-to-spec api
```

## What Gets Extracted

| Element      | From                         |
| ------------ | ---------------------------- |
| Descriptions | Docstrings                   |
| Types        | Type hints                   |
| Parameters   | Signatures                   |
| Exceptions   | Docstrings, raise statements |
| Dependencies | Import statements            |
```
