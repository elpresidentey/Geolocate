const fs=require('fs');const s=fs.readFileSync('index.html','utf8');
const i=s.indexOf('<script type="module">');const j=s.lastIndexOf('</script>');
fs.writeFileSync(process.env.TEMP+'\\m.mjs',s.slice(i+23,j));
