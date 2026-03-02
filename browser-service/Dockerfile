FROM mcr.microsoft.com/playwright:v1.58.0-jammy

WORKDIR /app

# Dependencies first (layer cache)
COPY package*.json ./
RUN npm ci --omit=dev

# Source
COPY src/ ./src/

ENV PORT=4000
ENV HOST=0.0.0.0
ENV NODE_ENV=production

EXPOSE 4000

CMD ["node", "src/index.js"]
