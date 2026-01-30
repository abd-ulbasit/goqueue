# GoQueue Documentation Site

This directory contains the Jekyll-based documentation website for GoQueue, hosted on GitHub Pages.

## Local Development

### Prerequisites
- Ruby 2.7+
- Bundler

### Setup

```bash
cd website
bundle install
bundle exec jekyll serve
```

Visit http://localhost:4000 to preview the site.

## Structure

```
website/
├── _config.yml          # Jekyll configuration
├── _data/               # Data files (navigation, etc.)
├── _includes/           # Reusable components
├── _layouts/            # Page layouts
├── _sass/               # SCSS stylesheets
├── assets/              # Static assets (images, CSS, JS)
├── docs/                # Documentation pages
│   ├── getting-started/
│   ├── concepts/
│   ├── api-reference/
│   ├── configuration/
│   └── operations/
├── index.md             # Homepage
└── comparison.md        # Comparison with alternatives
```

## Deployment

The site is automatically deployed to GitHub Pages when changes are pushed to the `gh-pages` branch.

To deploy manually:

```bash
# Build the site
bundle exec jekyll build

# The site is built to _site/
```
