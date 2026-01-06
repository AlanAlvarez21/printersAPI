# Printer API with Ngrok Tunnel

This application provides a printing service that can be exposed publicly using ngrok.

## Features

- Zebra printer API integration
- Automatic ngrok tunnel creation when running locally
- Automatic redirect to https://wmsys.fly.dev
- Cross-platform tunnel access

## Prerequisites

1. **Ngrok Account**: You need an ngrok account (free or paid)
2. **Ngrok Authentication**: Authenticate your ngrok client:
   ```bash
   ngrok config add-authtoken YOUR_AUTH_TOKEN
   ```
3. **Ngrok Installation**: Install ngrok from https://ngrok.com/download

## Installation

```bash
npm install
```

## Usage

### Running with Tunnel (Development)

```bash
npm run tunnel
```

This will:
1. Start the Express server on port 3000
2. Create an ngrok tunnel to your local server
3. Display the public URL
4. Automatically redirect to https://wmsys.fly.dev

### Running Normally

```bash
npm start
```

## Ngrok Free Tier Limitations

Note that if you're using the free tier of ngrok:
- You can only have one tunnel session active at a time
- If you get an authentication error, make sure no other ngrok processes are running

## API Endpoint

- POST `/print` - Send ZPL code to Zebra printer

## Configuration

The application uses a specific ngrok domain: `pregeological-nonidentical-ines.ngrok-free.app`