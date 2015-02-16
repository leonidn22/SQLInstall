alter table public.TConditionSets ADD COLUMN StringValue varchar(256) not null default '' ENCODING DELTAVAL ;

alter table public.TConditionSets ALTER COLUMN Value SET DEFAULT 'NONE' ;

alter table public.TConditionSets DROP COLUMN FloatValue ;
