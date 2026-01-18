---
description: >
  Compare two code files/modules semantically with behavioral preservation to identify API and structural differences
---

# Semantic Code Comparison Command

**Task**: Compare `{{arg1}}` vs `{{arg2}}` semantically

**Goal**: Determine if code has same behavioral contracts AND preserves dependencies despite different implementation.

**Method**: Contract extraction + dependency extraction + behavioral equivalence scoring

**CRITICAL: Independent Extraction Required** - Extract contracts from BOTH files independently. NEVER pre-populate with "items to verify" or prime extractor with change knowledge. Targeted validation inflates scores.

---

## Workflow

1. Extract contracts + dependencies from Code A (PARALLEL)
2. Extract contracts + dependencies from Code B (PARALLEL)
3. Compare contract sets AND dependency graphs
4. Calculate behavioral equivalence: contract 40% + dependency 40% + flow 20%
5. Report: shared/unique contracts, preserved/broken dependencies, warnings

**CRITICAL**: Steps 1-2 MUST run in parallel (single message, two Task calls). Sequential wastes ~50% time.

**Key Insight**: Contract preservation ≠ Behavioral preservation. Identical signatures don't guarantee identical behavior if dependencies/control flow changed.

---

## Steps 1-2: Extract Contracts + Dependencies (PARALLEL)

Invoke BOTH extraction agents in single message with two Task tool calls.

**Agent Prompt Template** (for BOTH files):

```
**SEMANTIC CONTRACT AND DEPENDENCY EXTRACTION**

**File**: {{argN}}

**Task**: Extract all behavioral contracts AND dependencies from this file.

---

## Part 1: Contract Extraction

**Contract**: Behavioral guarantee consumers depend on (signature, type constraint, assertion, invariant, documented behavior).

**Types**:
1. **Function/Method**: signature, parameters, return type, exceptions, docstring promises
2. **Class**: public attributes, methods, inheritance, protocols
3. **Module**: exports (__all__), public functions/classes, module constants
4. **Type**: TypedDicts, Protocols, Generic constraints, Unions
5. **Invariant**: assertions, pre/postconditions
6. **Side Effect**: I/O, state mutations, external calls

**Extraction Rules**:
- Extract ALL public API (non-underscore)
- Include type hints and constraints
- Capture docstring promises (raises, returns, parameters)
- Note side effects (file I/O, network, database, state mutation)
- Include assertions and validation

**Normalization**:
- Parameter names → positional ordering
- Standardize types (Optional[X] → X | None)
- Extract return type (even if implicit)
- Capture exception types from raise statements

---

## Part 2: Dependency Extraction

**Types**:
1. **Import**: External packages, internal modules, specific imports
2. **Call**: Functions/methods called within each function
3. **Inheritance**: Parent classes, mixins, protocols
4. **Composition**: Objects instantiated, attributes accessed
5. **Data Flow**: Parameters passed between functions, return value usage
6. **Control Flow**: Conditional calls, loop-dependent execution

---

## Part 3: Control Flow Extraction

**Elements**:
1. **Branch Points**: if/elif/else conditions and consequences
2. **Loop Structures**: for/while conditions, break/continue patterns
3. **Exception Handling**: try/except/finally structure, caught types
4. **Early Returns**: Guard clauses, validation returns
5. **Error Paths**: Exception raises, error return values

---

## Output Format (JSON)

```json
{
  "contracts": [
    {
      "id": "contract_1",
      "type": "function|method|class|module|type|invariant|side_effect",
      "name": "function_name",
      "signature": "def func(param: Type) -> ReturnType",
      "location": {"file": "path", "line_start": 10, "line_end": 25},
      "parameters": [{"name": "param", "type": "Type", "default": null, "description": "from docstring"}],
      "return_type": "ReturnType",
      "return_description": "what it returns",
      "exceptions": ["ValueError", "TypeError"],
      "side_effects": ["writes to file", "modifies self.state"],
      "docstring_promises": ["Does X", "Returns Y when Z"],
      "assertions": ["assert param > 0"],
      "visibility": "public|private|protected"
    }
  ],
  "dependencies": [
    {
      "id": "dep_1",
      "type": "import|call|inherit|compose|data_flow|control_flow",
      "from": "contract_1",
      "to": "external.module.function",
      "context": "called in main loop",
      "required": true,
      "location": {"line": 15}
    }
  ],
  "control_flow": {
    "branches": [{"condition": "if x > 0", "true_path": ["contract_2"], "false_path": ["contract_3"]}],
    "loops": [{"type": "for", "iterable": "items", "body_calls": ["contract_4"]}],
    "exception_handlers": [{"try_calls": ["contract_5"], "except_type": "ValueError", "handler_calls": ["contract_6"]}]
  },
  "metadata": {
    "total_contracts": 10,
    "public_contracts": 8,
    "total_dependencies": 15,
    "dependency_types": {"import": 5, "call": 8, "inherit": 2},
    "lines_of_code": 150,
    "cyclomatic_complexity": 12
  }
}
```

**CRITICAL**: Extract ALL contracts/dependencies, not just obvious ones.
```

**Execute PARALLEL extraction**:
```bash
# Single message with TWO Task tool calls
Task(subagent_type="general-purpose", model="sonnet", description="Extract contracts from Code A", prompt="...")
Task(subagent_type="general-purpose", model="sonnet", description="Extract contracts from Code B", prompt="...")

# Wait for both, save to /tmp/compare-code-{a,b}-extraction.json
```

---

## Step 3: Compare Contracts AND Dependencies

**Agent Prompt**:
```
**SEMANTIC COMPARISON WITH DEPENDENCY ANALYSIS**

**Code A Data**: {{CODE_A_DATA}}
**Code B Data**: {{CODE_B_DATA}}

**Task**: Compare contracts AND dependencies to determine behavioral equivalence.

---

## Contract Comparison Rules

1. **Exact Match**: Identical signature, types, exceptions → shared
2. **Semantic Equivalence**: Different implementation, identical contract → shared
3. **Signature Change**: Same name, different parameters/return → breaking
4. **Behavioral Change**: Same signature, different documented behavior → flag
5. **Unique**: Contracts in only one file

**Matching**:
- Functions: name + parameter types + return type
- Methods: class + method name + signature
- Classes: name + public interface (methods + attributes)
- Types: structure (TypedDict fields, Protocol methods)

---

## Dependency Comparison

1. **Preserved**: Same type, same from/to → maintained
2. **Broken**: In A not B → dependency removed
3. **Added**: In B not A → new dependency
4. **Changed**: Same endpoints, different type → modified

**Breaking Dependencies**:
- Removed imports that were used
- Removed function calls in critical paths
- Changed inheritance hierarchy
- Removed error handling

---

## Behavioral Equivalence Scoring

```python
def calculate_behavioral_equivalence(contract_comp, dep_comp, flow_comp):
    weights = {"contract_preservation": 0.4, "dependency_preservation": 0.4, "flow_preservation": 0.2}
    scores = {
        "contract_preservation": len(shared_contracts) / len(all_a_contracts),
        "dependency_preservation": len(preserved_deps) / len(all_a_deps),
        "flow_preservation": flow_similarity_score
    }
    base_score = sum(weights[k] * scores[k] for k in weights.keys())

    if contract_comp["breaking_changes"] > 0:
        base_score *= 0.5  # Heavy penalty for API breaks
    if dep_comp["critical_deps_broken"] > 0:
        base_score *= 0.7  # Penalty for broken dependencies

    return base_score
```

**Interpretation**:
- ≥0.95: Behaviorally equivalent - safe refactoring
- 0.85-0.94: Mostly equivalent - review carefully
- 0.70-0.84: Significant differences - behavior may differ
- <0.70: BREAKING - behavior will differ substantially

---

## Warning Generation

| Warning Type        | Severity | Trigger                           |
| ------------------- | -------- | --------------------------------- |
| API Break           | CRITICAL | Public function signature changed |
| Type Change         | HIGH     | Return/parameter type changed     |
| Dependency Removed  | HIGH     | Import/call removed               |
| Side Effect Change  | HIGH     | New/removed I/O, state mutation   |
| Exception Change    | MEDIUM   | Different exceptions raised       |
| Control Flow Change | MEDIUM   | Branch logic altered              |
| Deprecation         | LOW      | Deprecated method removed         |

```json
{
  "severity": "CRITICAL|HIGH|MEDIUM|LOW",
  "type": "api_break|type_change|dependency_removed|side_effect_change|exception_change|flow_change",
  "description": "Human-readable description",
  "affected_contracts": ["contract_1", "contract_2"],
  "recommendation": "Specific action to fix"
}
```

---

## Output Format (JSON)

```json
{
  "behavioral_equivalence_score": 0.85,
  "components": {"contract_preservation": 0.95, "dependency_preservation": 0.80, "flow_preservation": 0.75},
  "shared_contracts": [{"name": "function_name", "code_a_id": "c1", "code_b_id": "c3", "match_type": "exact|semantic"}],
  "unique_to_a": [{"name": "removed_func", "id": "c5", "severity": "HIGH", "reason": "public API removed"}],
  "unique_to_b": [{"name": "new_func", "id": "c7", "severity": "LOW", "reason": "new public API added"}],
  "breaking_changes": [{"contract": "func_name", "change": "parameter type changed", "a_signature": "...", "b_signature": "..."}],
  "dependency_changes": {"preserved": 12, "broken": 3, "added": 2, "critical_broken": ["import removed: pandas"]},
  "warnings": [{"severity": "CRITICAL", "type": "api_break", "description": "..."}],
  "summary": {
    "total_contracts_a": 10,
    "total_contracts_b": 9,
    "shared_count": 8,
    "breaking_changes_count": 1,
    "deps_preserved_pct": 0.80,
    "behaviorally_equivalent": false
  }
}
```
```

---

## Step 4: Generate Human-Readable Report

```markdown
## Code Comparison: {{arg1}} vs {{arg2}}

### Behavioral Equivalence Summary
- **Score**: {score}/1.0 ({interpretation})
- **Contract Preservation**: {contract_score}/1.0 ({shared}/{total} contracts)
- **Dependency Preservation**: {dep_score}/1.0 ({preserved}/{total} dependencies)
- **Control Flow**: {flow_score}/1.0

### Breaking Changes ({breaking_count})
{for each breaking change:}
**{severity}**: `{contract_name}` - {description}
- **Before**: `{a_signature}`
- **After**: `{b_signature}`
- **Impact**: {impact description}

### Warnings ({warning_count})
{for each CRITICAL/HIGH warning:}
**{severity}**: {description}
- **Affected**: {affected_contracts}
- **Recommendation**: {recommendation}

### Broken Dependencies ({broken_count})
{id}. **{type}**: {from} → {to}
   - Used in: {context}
   - Impact: {impact}

### Shared Contracts ({shared_count})
{id}. `{name}` - Match: {match_type}

### Unique to {{arg1}} ({unique_a_count})
{id}. `{name}` - {severity}: {reason}

### Unique to {{arg2}} ({unique_b_count})
{id}. `{name}` - {reason}

### Analysis
**Recommendation**: {SAFE_REFACTOR|REVIEW_REQUIRED|BREAKING_CHANGE}
```

---

## Score Thresholds

| Score         | Decision            | Characteristics                                                 |
| ------------- | ------------------- | --------------------------------------------------------------- |
| **≥0.95**     | SAFE REFACTOR       | Behaviorally equivalent, no API breaks, dependencies maintained |
| **0.85-0.94** | REVIEW REQUIRED     | Minor changes, check side effects/edge cases                    |
| **0.70-0.84** | SIGNIFICANT CHANGES | API/behavior changes, needs testing                             |
| **<0.70**     | BREAKING CHANGE     | Substantial differences, not drop-in replacement                |

---

## Use Cases

**Best for**: Refactoring validation, migration assessment, PR review, version comparison, duplicate detection

**Also works for**: Test coverage gap analysis, API compatibility checking, documentation verification

---

## Example Usage

```
/code-compare original.py refactored.py
/code-compare HEAD~1:file.py HEAD:file.py
```

---

## Limitations

1. Cannot execute code to verify runtime behavior
2. Dynamic behavior (metaprogramming, eval) may not be captured
3. Implicit contracts from usage patterns may be missed
4. External side effects require documentation to detect
5. Test coverage not analyzed (see /test-coverage)
