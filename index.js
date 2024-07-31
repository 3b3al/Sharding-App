const express = require ("express");
const app = express();
const {Client} = require("pg");
const consistantHash = require("consistent-hash");
const crypto = require("crypto");
const { url } = require("inspector");
const HashRing = require("hashring");
const hr = new HashRing();

hr.add("5431");
hr.add("5433");
hr.add("5434");

const clients = {
    "5431" : new Client({
        "host" :"localhost",
        "database":"postgres",
        "port":"5431",
        "user":"postgres",
        "password":"root2"
    }),
    "5433" : new Client({
        "host" :"localhost",
        "database":"postgres",
        "port":"5433",
        "user":"postgres",
        "password":"root2"
    }),
    "5434" : new Client({
        "host" :"localhost",
        "database":"postgres",
        "port":"5434",
        "user":"postgres",
        "password":"root2"
    })
}
connect();

async function connect() {
    clients["5431"].connect();
    clients["5433"].connect();
    clients["5434"].connect();
}

app.get("/:urlId" , async (req , res) =>{

    const urlId = req.params.urlId;
    const server = hr.get(urlId);
    const result = await clients[server].query("SELECT * FROM URL_TABLE WHERE url_id = $1" , [urlId]);

    if (result.rowCount > 0) {
        res.send({
            "urlId":urlId,
            "url":result.rows[0],
            "server":server
    
        })
    }
    else{
        res.sendStatus(404);
    }

})

app.post("/" ,async (req , res) => {

    const url = req.query.url;
    const hash = crypto.createHash("sha256").update(url).digest("base64");
    const urlId = hash.substring(0,5);
    const server = hr.get(urlId);

    await clients[server].query("INSERT INTO URL_TABLE (url , url_id) VALUES ($1 , $2)" , [url , urlId])
    res.send({
        "urlId":urlId,
        "url":url,
        "server":server

    })
})

app.listen(8089 , ()=> console.log("test listening on port 8089"))