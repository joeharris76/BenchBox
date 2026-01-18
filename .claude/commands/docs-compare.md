---
description: >
  Compare two documents semantically with relationship preservation to identify content and structural differences
---

# Semantic Document Comparison Command

**Task**: Compare `{{arg1}}` vs `{{arg2}}` for semantic equivalence with relationship preservation.

**Method**: Claim extraction + relationship extraction + execution equivalence scoring (claim 40% + relationship 40% + graph 20%)

**CRITICAL**: Extract claims from BOTH documents independently in parallel. NEVER pre-populate with "items to verify" - targeted validation inflates scores.

**Reproducibility**: Use `temperature=0`. Expected variance: ±0-1% same session, ±1-2% different sessions. Focus on score ranges not exact decimals.

---

## Workflow

1. Extract claims + relationships from Document A (IN PARALLEL)
2. Extract claims + relationships from Document B (IN PARALLEL)
3. Compare claim sets AND relationship graphs (after both complete)
4. Calculate execution equivalence score
5. Report: shared/unique claims, preserved/lost relationships, warnings

**CRITICAL**: Steps 1-2 MUST run in parallel (single message, two Task calls). Sequential extraction wastes ~50% time.

**Key Insight**: Claim preservation ≠ Execution preservation. Identical claims but lost relationships = different execution.

---

## Steps 1-2: Extract Claims + Relationships (PARALLEL)

Invoke BOTH extraction agents in a single message with two Task tool calls.

**Agent Prompt Template** (use for BOTH documents):

```
**SEMANTIC CLAIM AND RELATIONSHIP EXTRACTION**
**Document**: {{argN}}

## Part 1: Claim Extraction

**Claim Types**:
1. **Simple**: requirement, instruction, constraint, fact, configuration
2. **Conjunctions**: ALL of {X, Y, Z} (markers: "ALL", "both X AND Y")
3. **Conditionals**: IF condition THEN consequence_true ELSE consequence_false
4. **Consequences**: Actions from conditions (markers: "results in", "causes")
5. **Negations**: Prohibition with scope (markers: "NEVER", "prohibited", "CANNOT")

**Rules**: Atomic claims, extract ALL including implicit, minimal context, skip examples/meta-commentary.
**Normalization**: Present tense, imperative/declarative, normalize synonyms (must/required→must, prohibited/forbidden→prohibited), standardize negation ("must not X"→"prohibited to X").

## Part 2: Relationship Extraction

**Relationship Types**:
1. **Temporal**: Step A → Step B (markers: "before", "after", "then", "depends on")
2. **Prerequisite**: Condition → Action (markers: "prerequisite", "required before")
3. **Hierarchical Conjunctions**: ALL of X (markers: "ALL", "both...AND...", nested lists)
4. **Conditional**: IF-THEN-ELSE (markers: "IF...THEN...ELSE", "when X, do Y")
5. **Exclusion**: A and B CANNOT co-occur (markers: "CANNOT run concurrently", "mutually exclusive")
6. **Escalation**: State A → State B under trigger (markers: "escalate to", "upgrade severity")
7. **Cross-Document**: Doc A → Doc B Section X (markers: "see Section X.Y", "defined in Document Z")

## Output Format (JSON)

```json
{
  "claims": [
    {"id": "claim_1", "type": "simple|conjunction|conditional|consequence|negation", "text": "normalized claim", "location": "line/section", "confidence": "high|medium|low", "sub_claims": ["claim_2"], "condition": "condition text", "true_consequence": "claim_4", "false_consequence": "claim_5"}
  ],
  "relationships": [
    {"id": "rel_1", "type": "temporal|prerequisite|conditional|exclusion|escalation|cross_document", "from_claim": "claim_1", "to_claim": "claim_2", "constraint": "must occur after|required before|IF-THEN|CANNOT co-occur", "strict": true, "evidence": "line numbers and quote", "violation_consequence": "what happens if violated"}
  ],
  "dependency_graph": {"nodes": ["claim_1"], "edges": [["claim_1", "claim_2"]], "topology": "linear_chain|tree|dag|cyclic", "critical_path": ["claim_1", "claim_2"]},
  "metadata": {"total_claims": 10, "total_relationships": 5, "relationship_types": {"temporal": 3, "conditional": 1}}
}
```

**Execute**: Single message with TWO Task calls → save to /tmp/compare-doc-{a,b}-extraction.json
```

---

## Step 3: Compare Claims AND Relationships

**Agent Prompt**:

```
**SEMANTIC COMPARISON WITH RELATIONSHIP ANALYSIS**
**Document A Data**: {{DOC_A_DATA}}
**Document B Data**: {{DOC_B_DATA}}

## Claim Comparison Rules

1. **Exact Match**: Identical normalized text → shared
2. **Semantic Equivalence**: Different wording, identical meaning → shared
3. **Type Mismatch**: Same concept, different structure → flag structural change
4. **Unique**: Claims in only one document

**Enhanced Matching**:
- Conjunctions: Equivalent ONLY if same sub-claims AND all_required matches
- Conditionals: Equivalent ONLY if same condition AND same true/false consequences
- Consequences: Match on trigger AND impact
- Negations: Match on prohibition AND scope

## Relationship Comparison

1. **Exact Match**: Same type/from/to/constraint → preserved
2. **Missing**: In A, not B → lost
3. **New**: In B, not A → added
4. **Modified**: Same claims, different constraint → changed

**Preservation Score**:
```python
def calculate_relationship_preservation(a_rels, b_rels, shared_claims):
    a_valid = [r for r in a_rels if r.from in shared_claims and r.to in shared_claims]
    b_valid = [r for r in b_rels if r.from in shared_claims and r.to in shared_claims]
    preserved = count_matching_relationships(a_valid, b_valid)
    return preserved / len(a_valid) if a_valid else 1.0
```

## Execution Equivalence Scoring

```python
def calculate_execution_equivalence(claim_comp, rel_comp, graph_comp):
    weights = {"claim_preservation": 0.4, "relationship_preservation": 0.4, "graph_structure": 0.2}
    base_score = sum(weights[k] * scores[k] for k in weights.keys())
    if rel_comp["overall_preservation"] < 0.9:
        base_score *= 0.7  # Penalty for critical relationship loss
    return base_score
```

**Interpretation**:
- ≥0.95: Execution equivalent - minor differences acceptable
- 0.75-0.94: Mostly equivalent - review relationship changes
- 0.50-0.74: Significant differences - execution may differ
- <0.50: CRITICAL - execution will fail or produce wrong results

## Warning Generation

Generate warnings for: relationship loss, structural changes (conjunction split), conditional logic loss (IF-THEN-ELSE flattened), cross-reference breaks, exclusion constraint loss.

```json
{"severity": "CRITICAL|HIGH|MEDIUM|LOW", "type": "relationship_loss|structural_change|contradiction|navigation_loss", "description": "...", "affected_claims": ["claim_1"], "recommendation": "action to fix"}
```

## Output Format (JSON)

```json
{
  "execution_equivalence_score": 0.75,
  "components": {"claim_preservation": 1.0, "relationship_preservation": 0.6, "graph_structure": 0.4},
  "shared_claims": [{"claim": "...", "doc_a_id": "claim_1", "doc_b_id": "claim_3", "similarity": 100}],
  "unique_to_a": [{"claim": "...", "doc_a_id": "claim_5"}],
  "unique_to_b": [{"claim": "...", "doc_b_id": "claim_7"}],
  "relationship_preservation": {"temporal_preserved": 3, "temporal_lost": 2, "conditional_preserved": 1, "conditional_lost": 0, "overall_preservation": 0.6},
  "lost_relationships": [{"type": "temporal", "from": "step_1", "to": "step_2", "risk": "HIGH"}],
  "structural_changes": [{"type": "conjunction_split", "original": "ALL of {A,B,C}", "risk": "MEDIUM"}],
  "warnings": [{"severity": "CRITICAL", "type": "relationship_loss", "description": "..."}],
  "summary": {"total_claims_a": 10, "total_claims_b": 10, "shared_count": 10, "unique_a_count": 0, "unique_b_count": 0, "relationships_preserved": 3, "relationships_lost": 2, "execution_equivalent": false}
}
```
```

---

## Step 4: Generate Report

```markdown
## Semantic Comparison: {{arg1}} vs {{arg2}}

### Execution Equivalence Summary
- **Score**: {score}/1.0 ({interpretation})
- **Claim Preservation**: {claim_score}/1.0 ({shared}/{total})
- **Relationship Preservation**: {rel_score}/1.0 ({preserved}/{total})
- **Graph Structure**: {graph_score}/1.0

### Warnings ({count})
**{severity}**: {description} | Affected: {claims} | Recommendation: {action}

### Lost Relationships ({count})
{id}. **{type}**: {from} → {to} | Constraint: {constraint} | Risk: {risk}

### Shared Claims ({count})
{id}. **{claim}** - Match: {similarity}%

### Unique to {{arg1}} ({count})
{id}. **{claim}** - {location}

### Unique to {{arg2}} ({count})
{id}. **{claim}** - {location}

### Analysis
**Recommendation**: {APPROVE/REVIEW/REJECT based on score}
```

---

## Score Thresholds

| Score | Decision | Characteristics |
|-------|----------|-----------------|
| **≥0.95** | APPROVE | Execution equivalent, minor cosmetic differences |
| **0.85-0.94** | REVIEW | Functional equivalence with abstraction risks (ambiguity, lost exclusivity, flattened conditionals) |
| **0.50-0.74** | REJECT | Significant differences, >40% critical relationships lost |
| **<0.50** | REJECT CRITICAL | Execution will fail, core logic destroyed |

**Key Vulnerabilities**:
- **0.85-0.94**: Abstraction ambiguity ("ALL of X" → separate statements read as "ANY"), lost mutual exclusivity, conditional flattening, implicit temporal dependencies, cross-reference navigation breaks
- **<0.75**: Above plus missing decision branches, omitted prerequisites, contradictory instructions, removed safety constraints

---

## Limitations

1. Extracts only explicitly stated relationships (heavily implied may be missed)
2. Domain knowledge may be needed for some relationships
3. Deeply nested conditionals may not be fully captured
4. Cannot follow external cross-document references
5. Structural analysis only, cannot execute to verify
