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
        { label: 'Guides', autogenerate: { directory: 'guide' } },
        {
          label: 'Configuration',
          items: [
            { label: 'Environment Variables', link: '/config/environment/' },
            { label: 'S3 Providers', link: '/config/s3-providers/' },
            { label: 'Logging', link: '/config/logging/' },
            { label: 'Deployment', link: '/config/deployment/' },
          ],
        },
        { label: 'How It Works', autogenerate: { directory: 'how-it-works' } },
      ],
      customCss: ['./src/styles/custom.css'],
    }),
  ],
});
