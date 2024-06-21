const express = require('express');
const cors = require('cors');
const app = express();
const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');

app.use(cors());
app.use(express.json());

app.post('/print', async (req, res) => {

  console.log('**************************************************');
  console.log('Etiqueta mandada:', req.body);
  console.log('**************************************************');
  const longitudName = req.body.name.length;

  function setName() {
    return longitudName > 13 && longitudName < 25 ? 10 : 100;
  }



  const zpl_code = `
  ^XA

  ^CI28
  ^MMT
  ^PW450
  ^LL0700
  ^LS50
  ^FO200,200^GB450,297,2
  ^FO0,10^A0N,30,30^^FS
  ^FO300,300^GB450,85,2
  ^FO0,10^A0N,30,30^^FS
  ^FO260,60^A0N,35,30^FD ${req.body.name ? req.body.name : '-'}^FS
  ^FO80,100^A0N,40,40^FD${req.body.clave_producto ? req.body.clave_producto : '-'}^FS
  ^FO80,150^A0N,40,40^FDPB: ${req.body.peso_bruto ? req.body.peso_bruto : '-'}kg^FS
  ^FO80,190^A0N,40,40^FDPN: ${req.body.peso_neto ? req.body.peso_neto : '-'}kg ^FS
  ^FO80,230^A0N,40,40^FDML: ${req.body.metros_lineales ? req.body.metros_lineales : '-'}mts ^FS
  ^FO100,280^BY2,2
  ^BCN,60,Y,N,N
  ^FO80,280^A0N,30,30^FD${req.body.lote ? req.body.lote : '-'}^FS
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
    'apikey': '31mA0UIAbKTGUMXm21ktVFAAf3emWyEQ',
    'tenant': '490b6deb9f9691080a640daada7d91e9',
    'Content-Type': 'multipart/form-data', // Add Content-Type
  };

  try {
    const response = await axios.post(url, formData, {
      headers: {
        ...formData.getHeaders(),
        ...headers,
      },
    });

  res.json({ 'message': 'ZPL file sent successfully' });    
  } catch (error) {
    res.status(500).json({ 'message': 'Error sending the ZPL file', 'error': error.message });
  }
});

app.listen(3000, () => {
  console.log(`Servidor en ejecución en el puerto 3000`);
});

module.exports = app;
