import express from 'express';
import path from 'path'
export const AboutHomes = express.Router()

AboutHomes.use(express.json())
AboutHomes.use(express.urlencoded({extended:true}))

AboutHomes.use('/',  (req,res)=>{
    
})