const { spawn, exec } = require('child_process');
const open = require('open');

// Function to start ngrok tunnel
async function startNgrok(port = 3000) {
  return new Promise((resolve, reject) => {
    // Check if ngrok is installed
    exec('ngrok --version', (error, stdout, stderr) => {
      if (error) {
        console.error('ngrok is not installed or not in PATH. Please install ngrok first.');
        console.log('To install ngrok:');
        console.log('  - Download from: https://ngrok.com/download');
        console.log('  - Or install via package manager (e.g., brew install ngrok)');
        reject('ngrok not found');
        return;
      }

      console.log('ngrok is installed, starting tunnel...');
      console.log('Make sure your ngrok account is authenticated with:');
      console.log('  ngrok config add-authtoken YOUR_AUTH_TOKEN');
      console.log('Or if using the free tier, make sure no other ngrok sessions are running.\n');

      // Use the specific ngrok domain you provided
      const ngrok = spawn('ngrok', ['http', '--domain', 'pregeological-nonidentical-ines.ngrok-free.app', port.toString()]);

      ngrok.stdout.on('data', (data) => {
        const output = data.toString();
        console.log(`ngrok: ${output}`);

        // Look for the tunnel URL in ngrok output
        if (output.includes('pregeological-nonidentical-ines.ngrok-free.app')) {
          const tunnelUrl = 'https://pregeological-nonidentical-ines.ngrok-free.app';
          console.log(`\nTunnel URL: ${tunnelUrl}`);
          console.log(`Redirecting to: https://wmsys.fly.dev\n`);

          // Open the redirect URL in browser
          open('https://wmsys.fly.dev').catch(err => {
            console.error('Failed to open browser:', err.message);
          });

          resolve(tunnelUrl);
        }
      });

      ngrok.stderr.on('data', (data) => {
        const errorOutput = data.toString();
        console.error(`ngrok error: ${errorOutput}`);

        if (errorOutput.includes('ERR_NGROK_108')) {
          console.log('\nAuthentication Error: Your ngrok account is limited to 1 simultaneous session.');
          console.log('Please:');
          console.log('  1. Stop any other running ngrok tunnels');
          console.log('  2. Or authenticate with a paid account using: ngrok config add-authtoken YOUR_TOKEN');
        }
      });

      ngrok.on('close', (code) => {
        console.log(`ngrok process exited with code ${code}`);
      });
    });
  });
}

module.exports = { startNgrok };