SetTimer WatchCursor, 100
CoordMode "Mouse", "Screen"

WinGetPos ,,&winWidth,&winHeight, "ahk_exe chrome.exe"
MsgBox "The window is at max_X" winWidth " max_Y" winHeight

WatchCursor()
{

    MouseGetPos &xpos, &ypos
    ToolTip("The cursor is at X" xpos " Y" ypos)

}