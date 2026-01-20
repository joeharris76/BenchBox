---
description: Research and develop outline for a planned blog post
---

# Research Blog Post Outline

Gather sources, run benchmarks, and develop detailed outline for a planned blog post.

## Arguments

`$ARGUMENTS`: Path to outline (`_blog/{series}/outlines/{slug}.md`), post slug/title, `--series={name}`, `--deep` (comprehensive research), or empty to show posts needing research.

## Instructions

1. **Locate/Create Outline**: If path provided, read existing. If slug/title, search `_blog/*/outlines/`. If new, use series template.
2. **Review Context**: Read series plan, style guide (`_blog/{project}_Blog_Style_Guide.md`), related posts.
3. **Conduct Research**:
   - **Primary**: Official docs, academic papers, industry reports, benchmark specs
   - **Secondary**: Technical blogs, conference talks, GitHub repos, community discussions
   - **Original**: Run benchmarks, gather pricing, compile feature matrices
4. **Develop Outline**: For each section: heading, key points, data/evidence needed, word count. Include: hook, thesis, section breakdown, conclusion, CTA.
5. **Document Sources**: Create `## References & Resources` and `## Research notes (not for publication)`. List gaps.
6. **Save Research** (if extensive): Create `{series}/research/{topic}.md`, link from outline.

## Output Format

```markdown
## Research Complete: {Title}

### Status
- **File**: `_blog/{series}/outlines/{slug}.md`
- **Readiness**: Ready for Draft / Missing Sources

### Sources Gathered
| Type      | Count | Quality |
| --------- | ----- | ------- |
| Primary   | X     | High    |
| Secondary | Y     | Medium  |
| Data      | Z     | High    |

### Key Findings
1. {Finding with citation}

### Outline Structure
1. **{Section 1}** (~XXX words)
   - Point A (source)
   - Point B (source)

### Gaps Remaining
- [ ] {Missing source/data}

### Next Steps
- Use `/blog-draft` to write
```

## Outline Template

```markdown
# {Title}

> {One-sentence summary}

**Series**: [{Name}](/blog/series/{series}) | **Category**: {Category}
**Reading Time**: ~X min | **TL;DR**: {2-3 sentences}

## Outline

### Introduction: {Hook}
**Hook**: {Opening} | **Problem**: {Exploring} | **Thesis**: {Argue/discover}

### Section 1: {Title}
**Key points**: - Point 1 (cite) - Point 2 (cite)

### Conclusions
1. {Takeaway 1}
**Actions**: For {audience}: {advice}

## References & Resources
- [Source](url) - Description

## Research notes
### Key sources: 1. **{Source}** - {Provided}
### Gaps: - [ ] {Gap}
### Counterarguments: 1. "{Arg}" - Response: {response}

*Status: OUTLINE COMPLETE | Target: X,XXX words | Type: {Benchmark/Deep-Dive/Tutorial}*
```

## Example Usage

```
/blog-research _blog/business-of-analytics/outlines/cloud-ssd.md
/blog-research "Cloud SSD stagnation" --series=business-of-analytics
/blog-research cloud-ssd --deep
```
