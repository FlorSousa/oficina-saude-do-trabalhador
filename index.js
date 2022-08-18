import express from 'express';
import dotenv from 'dotenv';
import cors from 'cors';
import {Server} from 'socket.io'
import http from "http"
import path from 'path';
dotenv.config();

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({
    extended: true
}));



app.listen(process.env.PORT, i=> console.log("Servidor Rodando"))