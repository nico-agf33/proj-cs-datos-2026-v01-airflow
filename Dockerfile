FROM astrocrpublic.azurecr.io/runtime:3.3-7

ENV PYTHONPATH="/usr/local/airflow/include:${PYTHONPATH}"

USER root
RUN PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/ms-playwright \
    playwright install --with-deps chromium \
    && chmod -R a+rX /usr/local/share/ms-playwright

ENV PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/ms-playwright

USER astro
