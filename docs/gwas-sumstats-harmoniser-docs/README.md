# Website

This online documentation for the **GWAS Catalog Harmonisation Pipeline**([gwas-sumstats-harmoniser](https://github.com/EBISPOT/gwas-sumstats-harmoniser)) is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

## Local Development

### Prerequisites
[Node.js](https://nodejs.org/en/download/package-manager) version 18.0 or above
- You can check the installed version by running `node -v`
- You can use [nvm](https://github.com/nvm-sh/nvm) to manage  Node versions.

### Modifying the Docs or Blogs
1. To learn how to edit the documentation or blog posts, refer to the [Docusaurus](https://docusaurus.io/docs/category/guides).
2. For practical examples and frequently used features, the [Docusaurus tutorial](https://tutorial.docusaurus.io/docs/intro/) provides quick instructions.

### How to preview the page locally

The `npm run start` (or `yarn start`, `pnpm run start`) command builds your website locally and serves it through a development server, ready for you to view at http://localhost:3000/.

## GitHub Pages Deployment
### Github Action
This repository uses GitHub Actions, specifically the `action/upload-pages-artifact`, to automatically deploy the website to GitHub Pages whenever updates are pushed to the master branch.

To set up deployment, we followed the [Docusaurus-deploying-to-github-pages](https://docusaurus.io/docs/deployment#deploying-to-github-pages).

### GitHub Repository Settings:
Navigate to Settings -> Pages -> Build and Deployment -> Source -> GitHub Actions. Here you can check the workflow run details.

## Structure
1. All documentations markdown files are under the folder of `/docs` (more details: [docusaurus-Docs](https://docusaurus.io/docs/create-doc))
2. All blog markdown files are udner the folder of `/blog` (more details: [docusaurus-Blog](https://docusaurus.io/docs/blog))
3. `docusaurus.config.tc` file contains key configurations for your Docusaurus site and is located in the root directory. This file allows you to customize various aspects of your site, including icons, navigation bar, website name, URL, github link, theme and footer.