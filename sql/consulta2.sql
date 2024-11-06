-- Consulta para listar as empresas com o maior volume total negociado em 2023
SELECT 
    d.nome_empresa,
    SUM(f.volume) AS volume_total
FROM 
    fato_cotacoes f
    JOIN dim_empresa d ON f.id_empresa = d.id_empresa
    JOIN dim_data da ON f.id_data = da.id_data
WHERE 
    da.ano = 2023
GROUP BY 
    d.nome_empresa
ORDER BY 
    volume_total DESC
LIMIT 10;
