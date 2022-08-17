import express from 'express';
import path from 'path'
export const HomeRoutes = express.Router()

HomeRoutes.use(express.json())
HomeRoutes.use(express.urlencoded({extended:true}))

HomeRoutes.use('/',  (req,res)=>{
    
})