# The old Render service, reduced to a redirect to the Azure deployment.
# Build context: deploy/render-redirect. See server.js.
FROM node:22-alpine
WORKDIR /app
COPY server.js ./
USER node
EXPOSE 10000
CMD ["node", "server.js"]
