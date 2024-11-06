-- Consulta para encontrar a média de preços de abertura de cada mês em um ano específico para uma determinada empresa
SELECT 
    d.mes,
    AVG(f.preco_abertura) AS media_abertura
FROM 
    fato_cotacoes f
JOIN 
    dim_data d ON f.id_data = d.id_data
JOIN 
    dim_empresa e ON f.id_empresa = e.id_empresa
WHERE 
    e.ticker = 'VALE3'
    AND d.ano = 2023
GROUP BY 
    d.mes
ORDER BY 
    d.mes;
