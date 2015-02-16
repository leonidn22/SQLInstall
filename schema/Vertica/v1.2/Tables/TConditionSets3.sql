CREATE TABLE public.TConditionSets3
(
    RunId int NOT NULL,
    ConditionSetID int NOT NULL,
    ConditionName varchar(50) NOT NULL,
    Value varchar(50),
    FloatValue float,
    MeasuringUnit int
);

CREATE PROJECTION public.TConditionSets3
(
 RunId ENCODING DELTAVAL,
 ConditionSetID ENCODING DELTAVAL,
 ConditionName ENCODING RLE,
 Value,
 FloatValue,
 MeasuringUnit ENCODING DELTAVAL
)
AS
 SELECT ConditionSets.RunId,
        ConditionSets.ConditionSetID,
        ConditionSets.ConditionName,
        ConditionSets.Value,
        ConditionSets.FloatValue,
        ConditionSets.MeasuringUnit
 FROM public.TConditionSets3 as ConditionSets
 ORDER BY ConditionSets.ConditionName,
          ConditionSets.Value
UNSEGMENTED ALL NODES;


SELECT MARK_DESIGN_KSAFE( 0 );

