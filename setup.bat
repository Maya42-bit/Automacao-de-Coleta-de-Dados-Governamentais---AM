@echo off
echo Verificando instalação do Python...
where python >nul 2>nul || (
    echo ❌ Python não encontrado! Instale o Python antes de continuar.
    exit /b
)

echo ✅ Python encontrado!

echo Criando ambiente virtual...
python -m venv venv

echo Ativando ambiente virtual...
call venv\Scripts\activate

echo Instalando dependências...
pip install --upgrade pip
pip install requests pandas tqdm

echo ✅ Instalação concluída com sucesso!
echo Agora, para rodar o script, use:
echo   venv\Scripts\activate   (ativa o ambiente virtual)
echo   python seu_script.py     (executa o código)
