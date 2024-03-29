import http from "http";

async function init(){
    const httpServer = http.createServer();
    const PORT = process.env.PORT ? process.env.PORT : 8000;

    httpServer.listen(PORT, () =>
        console.log(`HTTP Server Started at PORT:${PORT}`)
        );
}
init();
