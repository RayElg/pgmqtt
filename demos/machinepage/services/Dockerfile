FROM node:20-slim

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

EXPOSE 9001
CMD ["node", "index.js"]
