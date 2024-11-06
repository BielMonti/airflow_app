-- Consulta para obter a variação média do preço de fechamento por trimestre
SELECT 
    da.ano,
    da.trimestre,
    AVG(f.preco_fechamento - f.preco_abertura) AS variacao_media_trimestral
FROM 
    fato_cotacoes f
    JOIN dim_data da ON f.id_data = da.id_data
GROUP BY 
    da.ano, da.trimestre
ORDER BY 
    da.ano, da.trimestre;
