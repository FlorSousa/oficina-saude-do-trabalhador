import express from 'express';
import dotenv from 'dotenv';
import cors from 'cors';
import { HomeRoutes } from './routes/home.js'
import { AboutHomes } from './routes/about.js'
dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({
    extended: true
}));

app.use('/', HomeRoutes)
app.use("/sobre", AboutHomes)

app.listen(8000, i=> console.log("Servidor Rodando"))