# Multi-stage Dockerfile for NestJS client microservices
FROM node:22-alpine AS builder

WORKDIR /usr/src/app

COPY package.json yarn.lock* package-lock.json* ./

RUN npm install

COPY . .

RUN npm run build

# Runner stage
FROM node:22-alpine

WORKDIR /usr/src/app

COPY package.json ./

RUN npm install --only=production

COPY --from=builder /usr/src/app/dist ./dist

# Run NestJS application
CMD ["node", "dist/main.js"]
