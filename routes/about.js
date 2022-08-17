import express from 'express';
import {fileURLToPath} from 'url';
export const AboutRoutes = express.Router()

AboutRoutes.use(express.json())
AboutRoutes.use(express.urlencoded({extended:true}))

AboutRoutes.use((req,res)=>{
    const path = (fileURLToPath(import.meta.url)
    .split('\\routes\\about.js')[0] + "/templates/sobre.html")
    res.sendFile(path)
})