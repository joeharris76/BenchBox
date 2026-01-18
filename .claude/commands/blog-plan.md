---
description: Plan a new blog post or series of posts
---

# Plan Blog Post or Series

Plan new blog post or series with concept, audience, structure, and publishing strategy.

## Arguments

`$ARGUMENTS`: Topic/title ("Cloud SSD performance"), `series: {concept}` (new series), `--series={name}` (add to existing), `list`, or empty to review planned posts.

## Instructions

1. **Blog Structure**: `_blog/{series-name}/{outlines,drafts,research}/`, `{project}_Blog_Style_Guide.md`, `planning/`

2. **New Series**: Read style guide, review existing series plans, `mkdir -p _blog/{series-slug}/{outlines,drafts}`, create `series-plan.md` with: concept, audience, tone, post template, planned posts, cadence.

3. **Single Post**: Determine series, define concept (title, thesis, audience, type, length), create outline in `{series}/outlines/{post-slug}.md`.

4. **Check Conflicts**: Review existing outlines, check `planning/PRIORITY_UNDRAFTED_BLOGS.md`.

5. **Research Needs**: Sources needed, benchmarks to run, timeline.

## Output Format

```markdown
## Blog Planned

### Overview
- **Type**: Single Post / Series
- **Title**: {title} | **Series**: {name or Standalone}

### Thesis
> {One-sentence summary}

### Structure
1. {Section 1}: {description}

### Research Needs
- [ ] {item}

### Next Steps
- `/blog-research` to gather sources
```

## Series Plan Template

```markdown
# "{Series Name}" Content Plan

**Concept**: {about} | **Audience**: {who} | **Tone**: {style}
**Length**: {words} | **Cadence**: {frequency}

## Post Template
1. **Section 1** (XXX words) - Key points

## Planned Posts
| #   | Title | Status | Notes |
| --- | ----- | ------ | ----- |

## Key Themes
1. **Theme**: {description}
```

## Example Usage

```
/blog-plan The evolution of columnar storage
/blog-plan series: Platform Performance Shootouts
/blog-plan ARM benchmarks --series=arm-performance
```
