Loop read, "inputs.csv"
{
    LineNumber := A_Index
    Loop parse, A_LoopReadLine, "CSV"
    {
        Result := MsgBox("Field " LineNumber "-" A_Index " is:`n" A_LoopField "`n`nContinue?",, "y/n")
        if Result = "No"
            return
        id := StrSplit(A_LoopField, "/").pop()
        MsgBox "The last field is " id
    }
}