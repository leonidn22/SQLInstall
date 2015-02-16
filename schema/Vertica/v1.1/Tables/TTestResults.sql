

CREATE TABLE public.TTestResults
(
    RunID int NOT NULL,
    DieTestOrder int NOT NULL,
    TestType char(1) NOT NULL,
    TestID int NOT NULL,
    ConditionSetID int NOT NULL,
    LoadDate timestamp NOT NULL,
    FloatValue float,
    StringValue varchar(256),
    LoLimit float,
    HiLimit float,
    IsPass boolean NOT NULL,
    HasAlarm boolean NOT NULL,
    SuitePassed boolean NOT NULL,
    SuiteExecute boolean NOT NULL,
    TestedAfterStopBin boolean NOT NULL,
    Flags int,
    ExecutionOrder int NOT NULL DEFAULT (-1),
    RepeatitionOrder int NOT NULL DEFAULT (-1)
)
PARTITION BY (date_trunc('DAY', TTestResults.LoadDate));



CREATE PROJECTION public.TTestResults_SP_V0
(
 RunID ENCODING RLE,
 DieTestOrder ENCODING DELTAVAL,
 TestType ENCODING RLE,
 TestID ENCODING RLE,
 ConditionSetID ENCODING RLE,
 LoadDate ENCODING RLE,
 FloatValue ENCODING DELTARANGE_COMP,
 StringValue,
 LoLimit ENCODING RLE,
 HiLimit ENCODING RLE,
 IsPass ENCODING RLE,
 HasAlarm ENCODING RLE,
 SuitePassed ENCODING RLE,
 SuiteExecute ENCODING RLE,
 TestedAfterStopBin ENCODING RLE,
 Flags ENCODING RLE,
 ExecutionOrder ENCODING RLE,
 RepeatitionOrder ENCODING RLE
)
AS
 SELECT TTestResults.RunID,
        TTestResults.DieTestOrder,
        TTestResults.TestType,
        TTestResults.TestID,
        TTestResults.ConditionSetID,
        TTestResults.LoadDate,
        TTestResults.FloatValue,
        TTestResults.StringValue,
        TTestResults.LoLimit,
        TTestResults.HiLimit,
        TTestResults.IsPass,
        TTestResults.HasAlarm,
        TTestResults.SuitePassed,
        TTestResults.SuiteExecute,
        TTestResults.TestedAfterStopBin,
        TTestResults.Flags,
        TTestResults.ExecutionOrder,
        TTestResults.RepeatitionOrder
 FROM public.TTestResults
 ORDER BY TTestResults.RunID,
          TTestResults.TestType,
          TTestResults.TestID,
          TTestResults.FloatValue,
          TTestResults.StringValue
SEGMENTED BY hash(TTestResults.TestID) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);
