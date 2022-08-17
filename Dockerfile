#Atualiza ou baixa a versão 16 do node
FROM node:16

#Define o nome do diretorio de trabalho dentro do container
WORKDIR /app

#Cria a variavel de ambiente PATH usando o .bin do node_modules
ENV PATH /app/node_modules/.bin:$PATH

#Copia o package.json
COPY package.json ./

#Instala as dependencias
RUN yarn install


#Copia os dados do diretorio para o container
COPY . ./

#Comando para iniciar
CMD ["yarn", "dev"]