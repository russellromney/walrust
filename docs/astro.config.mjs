// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://walrust.dev',
  integrations: [
    starlight({
      title: 'Walrust',
      expressiveCode: {
        themes: ['rose-pine', 'rose-pine-dawn'],
      },
      logo: {
        src: './src/assets/logo.svg',
      },
      components: {
        ThemeSelect: './src/components/ThemeSelect.astro',
      },
      social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/russellromney/walrust' }],
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Quick Start', link: '/' },
            { label: 'Why Walrust?', link: '/start/why-walrust/' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'CLI Reference', link: '/guide/cli/' },
            { label: 'Python API', link: '/guide/python-api/' },
            { label: 'Multi-Database Sync', link: '/guide/multi-database/' },
            { label: 'Read Replicas', link: '/guide/read-replicas/' },
            { label: 'Deployment with App', link: '/guide/deployment-with-app/' },
            { label: 'Migration from Litestream', link: '/guide/migration-from-litestream/' },
            { label: 'FAQ', link: '/guide/faq/' },
            { label: 'Troubleshooting', link: '/guide/troubleshooting/' },
          ],
        },
        {
          label: 'Configuration',
          items: [
            { label: 'Configuration Reference', link: '/config/configuration-reference/' },
            { label: 'Environment Variables', link: '/config/environment/' },
            { label: 'S3 Providers', link: '/config/s3-providers/' },
            { label: 'Logging', link: '/config/logging/' },
            { label: 'Deployment', link: '/config/deployment/' },
          ],
        },
        { label: 'How It Works', autogenerate: { directory: 'how-it-works' } },
        {
          label: 'Benchmarks',
          items: [
            { label: 'Overview', link: '/benchmarks/' },
            { label: 'Methodology', link: '/benchmarks/methodology/' },
            { label: 'Results', link: '/benchmarks/results/' },
          ],
        },
      ],
      customCss: ['./src/styles/custom.css'],
    }),
  ],
});
