<p align="center">
  <a href="https://fl0.com/" target="blank">
    <img src="https://user-images.githubusercontent.com/88681427/217122968-e6132cad-1944-4ebe-9ec1-105af6a18c4f.png">
  </a>
</p>

<h2 align="center">Node.js Quickstart</h2>
<p align="center">Backend engineering, supercharged.</p>

## Overview

Use this repository to get up and running on FL0 with the following stack:

<table>
<tr>
  <th>Language</th>
  <td>Javascript</td>
</tr>
<tr>
  <th>Router</th>
  <td>Express</td>
</tr>
</table>

## Getting Started

Clone this repo and run the following commands from the project root:

1. `npm install`
2. `npm start`
3. Visit http://localhost:3000 to see your app running

## Printer & Scale GUI Application

This repository also includes a Python GUI application for controlling printers and scales:

### Features
- Printer control for TSC TX200 devices via USB
- Scale integration for serial weight measurements
- Real-time data display
- Label printing capabilities

### Installation
1. Create a virtual environment:
   ```bash
   python3 -m venv venv
   ```
2. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Usage
Run the GUI application:
```bash
python3 gui_demo.py
```

See [README_GUI.md](README_GUI.md) for detailed documentation.

## Deploying to FL0

Checkout our [Getting Started Guide](https://docs.fl0.com) in the FL0 documentation!

## Questions

If you have any questions about FL0 or this template codebase please head on over to our [Discord channel](https://discord.gg/AmmVTt9Jrw).

## Issues

Any issues or feature requests can be raised on the [Issues page](https://github.com/fl0zone/template-nodejs/issues) of this repo.

## License

This template repository is [MIT licensed](LICENSE).
