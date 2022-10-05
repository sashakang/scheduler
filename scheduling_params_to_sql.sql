DROP TABLE IF EXISTS mfr_scheduling_params;
CREATE TABLE mfr_scheduling_params (
    id INTEGER NOT NULL,
    item VARCHAR(255) NOT NULL,
    std_model_days INTEGER NOT NULL,
    std_mold_days_no_punch INTEGER NOT NULL,
    std_mold_days_with_punch INTEGER NOT NULL
);
INSERT INTO mfr_scheduling_params
VALUES 
    (200145, 'МР Перевод протяжки в отливку', 2, 5, 8),
    (200146, 'МР Незначительная реставрация', 2, 5, 8),
    (200166, 'МР Размножение модели', 1, 5, 8),
    (200147, 'МР Значительная реставрация, гладкотянутые панели', 5, 5, 8),
    (200148, 'МР Перекладка модели', 4, 5, 8),
    (200149, 'МР Геометрия из тяг, 3D панели', 3, 5, 8),
    (200150, 'МР Сложная/мелкая геометрия', 6, 5, 8),
    (200151, 'МР Плоская лепка, до 20 мм', 10, 5, 8),
    (200152, 'МР Средняя лепка, до 40 мм', 10, 5, 8),
    (200153, 'МР Сложная лепка, до 80 мм', 10, 5, 8),
    (200154, 'МР Скульптурная лепка', 15, 5, 8)
;
