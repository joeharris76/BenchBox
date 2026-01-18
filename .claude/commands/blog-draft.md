---
description: Draft a blog post from a researched outline
---

# Draft Blog Post

Write first draft from researched outline, following style guide.

## Arguments

`$ARGUMENTS`: Outline path (`_blog/business-of-analytics/outlines/cloud-ssd.md`), post slug/title, `--continue` (resume incomplete), `--section={name}` (specific section), or empty to show ready outlines.

## Instructions

1. **Locate Context**: Read outline, style guide (`_blog/{project}_Blog_Style_Guide.md`), series plan, partial draft if exists.

2. **Pre-Draft Checklist**: Outline complete, sources cited, gaps addressed, word count target, post type clear.

3. **Apply Style**:
   - **Voice**: Conversational but authoritative, technical but accessible, honest, community-oriented
   - **Tone by Type**: Benchmark (objective, data-driven), Deep-Dive (detailed), Tutorial (helpful), Analysis (thoughtful)

4. **Write Draft**:
   ```markdown
   # [Title with Specifics]
   > [One-sentence hook]
   **Reading Time**: ~X min | **TL;DR**: [2-3 sentences]
   ---
   ## Introduction
   ## Section 1: [Title]
   ## Conclusions
   ---
   ## References
   ```

   **Guidelines**: Lead with data, show work (reproducible), embrace nuance, use "we"/"you", define acronyms, short paragraphs (3-4 sentences).

5. **Required Elements**:
   - Benchmark posts: Test Environment (hardware, database, benchmark, methodology, limitations)
   - All posts: Code examples with `{project}` commands, tables for data, clear section breaks

6. **Save**: Write to `_blog/{series}/drafts/{slug}.md` with status footer.

## Output Format

```markdown
## Draft Complete: {Title}

### Details
- **File**: `_blog/{series}/drafts/{slug}.md`
- **Word Count**: X,XXX | **Status**: First Draft

### Structure
1. {Section 1} - XXX words

### Style Compliance
| Element    | Status  |
| ---------- | ------- |
| Voice/tone | Matches |
| Data-first | Backed  |

### Known Issues
- [ ] {Issue}

### Next Steps
- `/blog-critique` for review
```

## Section Tips

**Introduction** (300-500w): Surprising fact/question, why care, thesis preview, set expectations.

**Body** (400-800w each): Topic sentence, subheadings for 300+w, data/evidence, transitions.

**Conclusion** (200-400w): Summarize findings, actionable recommendations, memorable final thought.

## Example Usage

```
/blog-draft _blog/business-of-analytics/outlines/cloud-ssd.md
/blog-draft cloud-ssd --section=introduction
/blog-draft arm-performance --continue
```
