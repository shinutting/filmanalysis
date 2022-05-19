SELECT T.Title, N.Category2, AVG(N.Rating) AS Avg_Rating, AVG(N.Z_Score) AS Avg_Z_Score
FROM Top100_Ratings AS T               
LEFT JOIN Ratings_Normalised_by_critics_genre AS N                   
ON T.Title = N.Title
AND T.Critic = N.Critic
GROUP BY T.Title
ORDER BY Avg_Z_Score DESC