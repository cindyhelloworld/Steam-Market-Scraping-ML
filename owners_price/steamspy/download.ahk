
SetWorkingDir "C:\Users\leoti\Documents\Code\steamspy" ;change here!

SetKeyDelay 120

CoordMode "Pixel", "Screen"
CoordMode "Mouse", "Screen"

WinActivate "ahk_exe chrome.exe"
WinWaitActive "ahk_exe chrome.exe"
WinGetPos ,,&winWidth,&winHeight, "ahk_exe chrome.exe"

Loop Read, "inputs\repeat_inputs.csv"
{
    line_number := A_Index
    
    Loop Parse A_LoopReadLine, "CSV"
    {
        WinActivate "ahk_exe chrome.exe"
        WinWaitActive "ahk_exe chrome.exe"

        site_url := A_LoopField
        id := StrSplit(A_LoopField, "/").pop()
        Send "^l"
        Sleep 200
        Send site_url "{Enter}"
        
        ;Make sure Enter
        attempts := 0
        Loop {
            if (attempts > 180){
                MsgBox "Something went wrong when enter key! Stop at appid" id
                return
            }
            Sleep 500
            Send "{Enter}"
            attempts += 1
        } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\enter.png"))

        ;Loading Page
        attempts := 0
        Loop {
            if (attempts > 180){
                MsgBox "Something went wrong when loading page! Stop at appid" id
                return
            }
            Sleep 1000
            attempts += 1
        } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\finish.png"))
        ;MsgBox "The icon was found at " view_X "x" view_Y
        Sleep 2000

        if (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\access.png")){
            Loop {
                if (attempts > 120){
                    MsgBox "Something went wrong when finding login.png! Stop at appid " id
                    return
                } 
                Sleep 500
                attempts += 1
            } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\login.png"))
            MouseMove view_X, view_Y
            Sleep 200
            MouseMove view_X+5, view_Y+5
            Sleep 200
            Click "left"

            Sleep 2000

            Loop {
                if (attempts > 120){
                    MsgBox "Something went wrong when finding now.png! Stop at appid " id
                    return
                } 
                Sleep 500
                attempts += 1
            } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\now.png"))
            MouseMove 200, 760
            Sleep 200
            Click "left"
            Sleep 2000
            MouseMove view_X, view_Y
            Sleep 200
            Click "left"

            attempts := 0
            Loop {
                if (attempts > 180){
                    MsgBox "Something went wrong when loading page! Stop at appid" id
                    return
                }
                Sleep 1000
                attempts += 1
            } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\finish.png"))
            ;MsgBox "The icon was found at " view_X "x" view_Y
            Sleep 2000
        }

        ;download
        attempts := 0
        Loop {
            if (attempts > 120){
                MsgBox "Something went wrong when finding download.png! Stop at appid" id
                return
            }
            Sleep 500
            attempts += 1
        } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\download.png") OR ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\download_dark.png"))
        ;MsgBox "The icon was found at " view_X "x" view_Y

        ;view_X := 1750
        ;view_Y := 670
        rand_X := Random(1, 5)
        ;rand_Y := Random(1, 5)
        MouseMove view_X + rand_X, view_Y
        Sleep 200
        MouseMove view_X + 3, view_Y + 5

        ;Sleep 200

        ;save as
        attempts := 0
        Loop {
            if (attempts > 120){
                MsgBox "Something went wrong when finding save_as.png! Stop at appid " id
                return
            } 
            Sleep 500
            attempts += 1
        } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\save_as.png"))
        ;MsgBox "The icon was found at " view_X "x" view_Y

        ;view_X := 1600
        ;view_Y := 740
        rand_X := Random(1, 5)
        rand_Y := Random(1, 5)
        MouseMove view_X + rand_X, view_Y + rand_Y

        ;Sleep 200

        ;click csv
        attempts := 0
        Loop {
            if (attempts > 120){
                MsgBox "Something went wrong when finding csv.png! Stop at appid " id
                return
            }  
            Sleep 500
            attempts += 1
        } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\csv.png"))
        ;MsgBox "The icon was found at " view_X "x" view_Y

        ;view_X := 1500
        ;view_Y := 740
        rand_X := Random(1, 5)
        rand_Y := Random(1, 5)
        MouseMove view_X + rand_X, view_Y + rand_Y
        Sleep 200
        Click "left"

        ;rename file
        attempts := 0
        Loop {
            if (attempts > 120){
                MsgBox "Something went wrong when finding rename.png! Stop at appid " id
                return
            }
            Sleep 500
            attempts += 1
        } Until (ImageSearch(&view_X, &view_Y, 0, 0, winWidth, winHeight, "assets\rename.png"))

        Send id "{Enter}"

        Sleep 200
        MouseMove 0, 0

        Sleep Random(20, 30)*1000 + Random(100, 200)

    }
}
MsgBox "Done!"
ExitApp
