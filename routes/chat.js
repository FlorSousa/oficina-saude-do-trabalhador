import express from 'express';
import {fileURLToPath} from 'url';
export const ChatRoutes = express.Router()

ChatRoutes.use(express.json())
ChatRoutes.use(express.urlencoded({extended:true}))

ChatRoutes.use((req,res)=>{
    const path = (fileURLToPath(import.meta.url)
    .split('\\routes\\chat.js')[0] + "/views/index.html")
    res.sendFile(path)
})