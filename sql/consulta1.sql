-- Consulta para encontrar o preço de fechamento médio de uma empresa específica em um período determinado
SELECT 
    d.nome_empresa, 
    AVG(f.preco_fechamento) AS preco_medio_fechamento
FROM 
    fato_cotacoes f
    JOIN dim_empresa d ON f.id_empresa = d.id_empresa
    JOIN dim_data da ON f.id_data = da.id_data
WHERE 
    d.ticker = 'PETR4' 
    AND da.data BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY 
    d.nome_empresa;
