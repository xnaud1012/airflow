UPDATE test
SET test_id = %(test_id)s, 
    test_a = %(test_a)s, 
    test_b = %(test_b)s, 
    test_c = %(test_c)s
WHERE test_id = 'admin1'