const ytdl = require('ytdl-core');
const fs = require('fs');
ytdl('https://www.youtube.com/watch?v=dQw4w9WgXcQ', { filter: 'audioonly' })
  .pipe(fs.createWriteStream('test.mp3'));