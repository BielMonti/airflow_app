-- Consulta para encontrar os dias com o maior preço de fechamento para uma empresa
SELECT 
    d.nome_empresa,
    da.data,
    f.preco_fechamento
FROM 
    fato_cotacoes f
    JOIN dim_empresa d ON f.id_empresa = d.id_empresa
    JOIN dim_data da ON f.id_data = da.id_data
WHERE 
    d.ticker = 'VALE3' 
    AND da.ano = 2023
ORDER BY 
    f.preco_fechamento DESC
LIMIT 5;
