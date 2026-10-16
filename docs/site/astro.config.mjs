// @ts-check
import { defineConfig } from 'astro/config';
import { unified } from '@astrojs/markdown-remark';
import starlight from '@astrojs/starlight';
import starlightLlmsTxt from 'starlight-llms-txt';
import { remarkPrependBase } from './src/plugins/remark-prepend-base.mjs';

const BASE = '/leeway';

export default defineConfig({
  site: 'https://go-steer.github.io',
  base: BASE,
  markdown: {
    processor: unified({ remarkPlugins: [remarkPrependBase(BASE)] }),
  },
  integrations: [
    starlight({
      title: 'leeway',
      description:
        'Kubernetes topology placement-drift and ComputeClass priority-rank analyzer: one-shot multi-cluster CLI reports (leeway report) and a continuous incident watcher with HTTP webhook callbacks (leeway watch).',
      logo: undefined,
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/go-steer/leeway',
        },
      ],
      editLink: {
        baseUrl:
          'https://github.com/go-steer/leeway/edit/main/docs/site/',
      },
      head: [
        {
          tag: 'script',
          attrs: { 'is:inline': true },
          content: "document.documentElement.dataset.theme = 'light';",
        },
      ],
      plugins: [
        starlightLlmsTxt({
          projectName: 'leeway',
          description:
            'Kubernetes topology placement-drift and ComputeClass priority-rank analyzer: one binary with two execution modes — one-shot multi-cluster JSON reports (leeway report) and a continuous incident daemon (leeway watch) with HTTP webhook callbacks.',
          promote: ['agents', 'getting-started/**'],
          demote: ['reference/**'],
        }),
      ],
      customCss: ['./src/styles/theme.css'],
      components: {
        ThemeSelect: './src/components/ThemeSelect.astro',
        ThemeProvider: './src/components/ThemeProvider.astro',
      },
      sidebar: [
        {
          label: 'Overview',
          items: [
            { label: 'Introduction', link: '/' },
            { label: 'For AI agents', link: '/agents/' },
          ],
        },
        {
          label: 'Getting started',
          items: [{ autogenerate: { directory: 'getting-started' } }],
        },
        {
          label: 'Concepts',
          items: [{ autogenerate: { directory: 'concepts' } }],
        },
        {
          label: 'Operations',
          items: [{ autogenerate: { directory: 'operations' } }],
        },
        {
          label: 'Reference',
          items: [{ autogenerate: { directory: 'reference' } }],
        },
      ],
    }),
  ],
});
