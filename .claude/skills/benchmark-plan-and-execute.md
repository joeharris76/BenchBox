---
description: Full workflow from planning to implementation to testing
---

# Benchmark Plan and Execute

Complete workflow for implementing new features, benchmarks, or major changes.

## Instructions

When the user asks to implement a significant feature, create a new benchmark, or make major changes:

1. **Create a comprehensive plan**:

   **Planning Phase:**
   - Understand requirements thoroughly
   - Research existing implementations
   - Identify affected components
   - List all files that need changes
   - Define success criteria
   - Estimate complexity and time

   **Plan document should include:**
   ```markdown
   ## [Feature/Benchmark Name] Implementation Plan

   ### Overview
   [What is being implemented and why]

   ### Requirements
   1. [Requirement 1]
   2. [Requirement 2]
   ...

   ### Research Findings
   - Existing similar implementations
   - External specifications/documentation
   - Technical constraints
   - Dependencies

   ### Architecture
   [How it will fit into BenchBox]
   - New files to create
   - Existing files to modify
   - Integration points
   - Data flow

   ### Implementation Steps
   1. **[Phase 1 Name]** (Est: Xh)
      - Create/modify file1.py
      - Implement feature A
      - Add tests

   2. **[Phase 2 Name]** (Est: Xh)
      - ...

   ### Testing Strategy
   - Unit tests for X
   - Integration tests for Y
   - Compliance validation for Z

   ### Success Criteria
   - [ ] All tests pass
   - [ ] Documentation complete
   - [ ] Code reviewed
   - [ ] Performance acceptable

   ### Risks and Mitigation
   - Risk 1: [description] → Mitigation: [approach]
   - Risk 2: [description] → Mitigation: [approach]
   ```

2. **Save plan to `_project/`**:

   ```bash
   # Save as descriptive filename
   _project/[feature_name]_implementation_plan.md
   ```

3. **Present plan to user for approval**:

   ```markdown
   ## Implementation Plan Created

   I've created a comprehensive plan for [feature name].

   **Plan saved to:** `_project/[feature_name]_implementation_plan.md`

   **Summary:**
   - X new files to create
   - Y existing files to modify
   - Z tests to add
   - Estimated time: N hours

   **Key phases:**
   1. [Phase 1] - [brief description]
   2. [Phase 2] - [brief description]
   3. [Phase 3] - [brief description]

   **Would you like me to:**
   - Proceed with implementation
   - Revise the plan
   - Discuss specific aspects
   ```

4. **Wait for explicit user approval** before implementing:
   - User may want to review plan
   - User may want to modify approach
   - User may want to clarify requirements
   - Never implement without confirmation

5. **Execute implementation** (only after approval):

   **Use TodoWrite tool** to track progress:
   ```markdown
   - Phase 1: Setup and scaffolding
   - Phase 2: Core implementation
   - Phase 3: Testing and validation
   - Phase 4: Documentation
   ```

   **Follow BenchBox patterns:**
   - Use existing code as templates
   - Follow architecture guidelines
   - Maintain consistent style
   - Add comprehensive tests as you go

   **Mark todos as completed** immediately after finishing each:
   - Don't batch completions
   - Keep user informed of progress
   - Report any blockers immediately

6. **Run tests continuously**:

   After each major change:
   ```bash
   # Quick smoke test
   make test-fast

   # Specific tests for your changes
   uv run -- python -m pytest tests/path/to/relevant/tests.py -v
   ```

7. **Quality checks before completion**:

   ```bash
   make lint       # Check code style
   make format     # Format code
   make typecheck  # Type checking
   make test-all   # All tests (or at least relevant ones)
   ```

8. **Update documentation**:
   - Module docstrings
   - Function docstrings
   - README if user-facing changes
   - Examples if new API
   - Migration guides if breaking changes

9. **Create completion summary**:

   Save to `_project/[feature_name]_completion_summary.md`:

   ```markdown
   ## [Feature Name] - Implementation Complete

   ### What Was Implemented
   - [Feature 1]
   - [Feature 2]
   - [Feature 3]

   ### Files Created
   - path/to/new/file1.py
   - path/to/new/file2.py

   ### Files Modified
   - path/to/existing/file1.py (lines 45-67)
   - path/to/existing/file2.py (lines 123-145)

   ### Tests Added
   - tests/unit/test_new_feature.py (15 tests)
   - tests/integration/test_new_integration.py (8 tests)

   ### Test Results
   ✅ All 23 new tests passing
   ✅ All existing tests still passing (234 total)
   ✅ Code coverage: 94% → 95%

   ### Quality Checks
   ✅ Lint: No issues
   ✅ Format: All files formatted
   ✅ Typecheck: No type errors

   ### Documentation
   - Added docstrings to all new functions
   - Updated README with new examples
   - Created user guide in docs/

   ### Performance
   - Benchmark X: 1.2s → 0.8s (33% faster)
   - Memory usage: 150MB → 145MB

   ### Breaking Changes
   None

   ### Migration Notes
   N/A - fully backward compatible

   ### Next Steps
   - Consider adding feature Y
   - Monitor performance in production
   - Gather user feedback

   ### Known Limitations
   - [Limitation 1]
   - [Limitation 2]
   ```

10. **Update PROJECT_TODO.yaml**:
    - Mark related TODOs as complete
    - Move to PROJECT_DONE.yaml
    - Add any new follow-up TODOs discovered

11. **Final report to user**:

    ```markdown
    ## Implementation Complete! ✅

    **Feature:** [Name]
    **Files changed:** X new, Y modified
    **Tests:** All passing (23 new, 234 total)
    **Quality:** All checks passed

    **Key achievements:**
    - [Achievement 1]
    - [Achievement 2]
    - [Achievement 3]

    **Documentation:**
    - Plan: `_project/[name]_implementation_plan.md`
    - Summary: `_project/[name]_completion_summary.md`

    **Ready for:** Code review, commit, deployment

    **Would you like me to create a commit for these changes?**
    ```

## Common Workflows

**New Benchmark:**
1. Plan: Research spec, list queries, define schema
2. Implement: Create core files, integrate C tools if needed
3. Test: Unit tests, integration tests, compliance validation
4. Document: Usage examples, API reference

**Platform Adapter:**
1. Plan: Study platform docs, list required features
2. Implement: Adapter class, connection logic, dialect handling
3. Test: Connection, query execution, result validation
4. Document: Credential setup, usage examples

**Feature Enhancement:**
1. Plan: Analyze current implementation, design improvements
2. Implement: Modify existing code, maintain compatibility
3. Test: Existing tests still pass, new tests for new features
4. Document: Update docs, add migration guide if needed

**Bug Fix:**
1. Plan: Reproduce bug, identify root cause, design fix
2. Implement: Fix bug, add regression test
3. Test: Bug fixed, no regressions introduced
4. Document: Add test case, update changelog

## Best Practices

**Planning:**
- Research thoroughly before coding
- Break large tasks into phases
- Identify risks early
- Get user buy-in on approach

**Implementation:**
- Use TodoWrite to track progress
- Test continuously, not just at end
- Follow existing patterns
- Keep changes focused

**Testing:**
- Write tests as you code
- Test both success and error cases
- Use appropriate test markers
- Aim for high coverage

**Documentation:**
- Document as you code
- Include examples
- Explain "why" not just "what"
- Keep docs updated

**Communication:**
- Keep user informed of progress
- Report blockers immediately
- Ask for clarification when needed
- Celebrate completions

## Notes

- This is the most common workflow pattern in BenchBox development
- Planning prevents costly rework
- Continuous testing catches issues early
- Documentation is as important as code
- User approval is required before major changes
