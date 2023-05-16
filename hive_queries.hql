-- user connections with their names
create table result_table as
select
    uc.user_id, src.name as src_name, uc.target, trg.name as trg_name
from user_connections uc
join users src on uc.user_id = src.user_id
join users trg on uc.target = trg.user_id;


-- connection weights with user names
-- select
--     h.user_id, src.name, h.target, trg.name, h.weight
-- from heavyN h
-- join users src on h.user_id = src.user_id
-- join users trg on h.target = trg.user_id;

-- users with most popular photos
-- select
--     mpp.user_id, u.name, u.country
-- from most_popular_photos mpp
-- join users u on u.user_id = mpp.user_id;

