SELECT *
FROM layoffs;

CREATE TABLE layoffs_1
LIKE layoffs;

INSERT layoffs_1
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_1;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_1;

WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_1
)
SELECT * 
FROM duplicates_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_1
WHERE company = 'Casper';

CREATE TABLE `layoffs_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_2;

INSERT INTO layoffs_2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_1;

DELETE
FROM layoffs_2
WHERE row_num > 1;

SELECT company, TRIM(company)
FROM layoffs_2;

UPDATE layoffs_2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_2
ORDER BY 1;

SELECT *
FROM layoffs_2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT *
FROM layoffs_2;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_2
ORDER BY 1;

UPDATE layoffs_2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_2;

UPDATE layoffs_2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_2;

ALTER TABLE layoffs_2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT * 
FROM layoffs_2
WHERE company = "Bally's Interactive";

SELECT *
FROM layoffs_2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE layoffs_2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT t1.industry, t2.industry
FROM layoffs_2 t1
JOIN layoffs_2 t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = ' ')
AND t2.industry IS NOT NULL;

UPDATE layoffs_2 t1
JOIN layoffs_2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_2;


SELECT *
FROM layoffs_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_2;

ALTER TABLE layoffs_2
DROP COLUMN row_num;

SELECT *
FROM layoffs_2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_2;

SELECT *
FROM layoffs_2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_2;

SELECT country, SUM(total_laid_off)
FROM layoffs_2
GROUP BY country
ORDER BY 2 DESC;

SELECT *
FROM layoffs_2;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, AVG(percentage_laid_off)
FROM layoffs_2
GROUP BY company
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`, 1, 7) AS `Month`,  SUM(total_laid_off)
FROM layoffs_2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;

SELECT *
FROM layoffs_2;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `Month`,  SUM(total_laid_off) AS total_off
FROM layoffs_2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT `Month`, total_off
,SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_Total; 

SELECT company, SUM(total_laid_off)
FROM layoffs_2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`),  SUM(total_laid_off)
FROM layoffs_2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH company_year(company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`),  SUM(total_laid_off)
FROM layoffs_2
GROUP BY company, YEAR(`date`)
), 
company_year_rank AS
(
SELECT *, 
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT *
FROM company_year_rank
WHERE Ranking <= 5;












































































































