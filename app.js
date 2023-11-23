const express = require('express');
const cors = require('cors');
const app = express();
const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');

app.use(cors());
app.use(express.json());

app.post('/print', async (req, res) => {

  console.log('Petición recibida en el servidor');
  console.log('Body:', req.body);
  const longitudName = req.body.name.length;

  function setName() {
    return longitudName > 13 && longitudName < 25 ? 10 : 100;
  }

  const zpl_code = `
  ^XA
  ^CI28
  ^MMT
  ^PW400
  ^LL0500
  ^LS0
  ^FO${setName()},20^A0N,30,30^FD${req.body.name}^FS
  ^FO10,60^A0N,35,35^FD     ${req.body.clave_producto}^FS
  ^FO10,100^A0N,30,30^FDPB: ${req.body.peso_bruto}kg^FS
  ^FO10,135^A0N,30,30^FDPN: ${req.body.peso_neto}kg ^FS
  ^FO10,165^A0N,30,30^FDML: ${req.body.metros_lineales}m ^FS
  ^FO10,200^BY2,2
  ^BCN,80,Y,N,N
  ^FD${req.body.lote}^FS
  ^PQ1,0,1,Y
  ^XZ
  `;

  fs.writeFileSync('temp.zpl', zpl_code);
  const formData = new FormData();
  
  // TODO: Make the serial number of the printer dynamic
  formData.append('sn', 'D8N230701799');
  formData.append('zpl_file', fs.createReadStream('temp.zpl'));


  const url = 'https://api.zebra.com/v2/devices/printers/send';
  const headers = {
    'accept': 'text/plain',
    'apikey': process.env.APIKEY,
    'tenant': process.env.TENANT,
    'Content-Type': 'multipart/form-data', // Add Content-Type
  };


  try {
    const response = await axios.post(url, formData, {
      headers: {
        ...formData.getHeaders(),
        ...headers,
      },
    });

    if (response.status === 200) {
      res.json({ 'message': 'ZPL file sent successfully' });
    } else {
      res.status(response.status).json({ 'message': 'Error sending the ZPL file', 'status_code': response.status });
    }
  } catch (error) {
    res.status(500).json({ 'message': 'Error sending the ZPL file', 'error': error.message });
  }
});

app.listen(process.env.PORT, () => {
  console.log(`Servidor en ejecución en el puerto ${process.env.PORT}`);
});

module.exports = app;
