import express from 'express';
import dotenv from 'dotenv';
import cors from 'cors';
import { ChatRoutes } from './routes/chat.js'
import { AboutRoutes } from './routes/about.js'
dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({
    extended: true
}));

app.use('/chat', ChatRoutes)
app.use("/sobre", AboutRoutes)
app.get("/", (req,res) => res.send("Hello World!"))

app.listen(8000, i=> console.log("Servidor Rodando"))