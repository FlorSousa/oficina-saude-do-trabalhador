# Oficina do projeto Saude do Trablhador

### Aqui vocês vão encontrar um guia de como executar o código e o próprio código comentado por nós



## Antes de tudo
- Verifiquem se o .env está presente
- Verifique se o docker e docker-compose estão instalados
- Caso queira rodar o projeto localmente verifique se o postgres, o python e as bibliotecas listadas em requeriments.txt já estão instaladas


## Baixando os arquivos csv
``` 
  python download_csv.py
```

## Comandos para executar o docker

Para construir as imagens 
``` 
  docker-compose build
```

Para construir as imagens e executar o script
``` 
  docker-compose up 
```
