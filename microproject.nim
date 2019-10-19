import strutils, sequtils, httpclient, json, os
import system, algorithm, itertools, strformat

#########################
#   Type Declaration    #
#########################

type 
    Transaction = object
        rechtst: string
        quartal: string
        bekannt: int
        medienin: string
        euro: float

#gets quartal from Transaction
proc getQuarter(x: Transaction): string =
    x.quartal

proc getRechtstraeger(x: Transaction): string =
    x.rechtst

proc getMedieninhaber(x: Transaction): string =
    x.medienin


type
    Overview = object
        quartal: string
        p2: float
        p4: float
        p31: float

type Pair = object
    name: string
    euro: float

proc toPairwithSum(group: tuple[k: string, v: seq[Transaction]]): Pair =
    var
        name = group[0]
        sum = group[1].mapIt(it.euro).foldl(a+b)
    return Pair(name: name, euro: sum)
#########################
#   Procs for Features  #
#########################

proc printHelpMsg(): void =
    echo "'help' ... print this message" 
    echo "'exit' ... terminate this application"
    echo "'payers' ... print a list of all payers"
    echo "'recipients' ... print a list of all recipient"
    echo "'top' n 'payers'|'recipients' '§2'|'§4'|'§31' ... print the n biggest payers|recipients for the given payment type"
    echo "'search' 'payers'|'recipients' searchTerm ... print a list of all payers|recipients containing the given searchTerm"
    echo "'load' quarter1 quarter2 .. quartern ... load date for the given list of quarters"
    echo "'details' 'payers'|'recipients' organization ... print a list of all payments payed or received by the given payers/recipient"

proc toTransaction(json: JsonNode): Transaction =
    return Transaction(
                rechtst: json["rechtstraeger"].getStr,
                quartal: json["quartal"].getStr,
                bekannt: json["bekanntgabe"].getInt,
                medienin: json["mediumMedieninhaber"].getStr,
                euro: json["euro"].getFloat
    )

#Loads Data for ONE Quarter, by parsing json via http request
proc loadDataForQuarter(quarter: string): seq[Transaction] =
    var
        client = newHttpClient()
        url = "https://data.rtr.at/api/v1/tables/MedKFTGBekanntgabe.json?quartal=" & quarter & "&leermeldung=0&size=0"
        allData = client.getContent(url).parseJson
        rawData = allData["data"]
        quartalData: seq[Transaction] = @[]
    client.close()
    for entry in rawData:
        quartalData.add(entry.toTransaction())
        
    return quartalData

#Uses loadDataForQuarter multiple times for a list of quarters
proc loadMultiple(quarters: seq[string]): seq[Transaction] =
     quarters.map(loadDataForQuarter).foldl(concat(a,b))

#prints names of payers
proc payers(data: seq[Transaction]): void =
    var listOfPayers: seq[string] = data.map(getRechtstraeger).deduplicate()
    listOfPayers.sort(proc (x,y:string):int = result = cmp(x.toLowerAscii,y.toLowerAscii))
    for i in listOfPayers:
        echo i.indent(2)

#prints names of recipients
proc recipients(data: seq[Transaction]): void =
    var listOfRecipients: seq[string] = data.map(getMedieninhaber).deduplicate()
    listOfRecipients.sort(proc (x,y:string):int = result = cmp(x.toLowerAscii,y.toLowerAscii))
    for i in listOfRecipients:
        echo i.indent(2)


proc getSumByBekannt(data: seq[Transaction], bekannt: int): float =
    return data.filterIt(it.bekannt == bekannt).mapIt(it.euro).foldl(a + b)

#helper proc for quarters, casts data into Overview type
proc getOverview(quarter: seq[Transaction]): Overview =
    var 
        quartal = quarter[0].quartal
        p2 = getSumByBekannt(quarter, 2)
        p4 = getSumByBekannt(quarter, 4)
        p31 = getSumByBekannt(quarter, 31)
        
        foo = Overview(quartal: $quartal, p2: p2, p4: p4, p31: p31)
    return foo

#helper proc for quarters, prints one Overview formatted
proc printOverview(summary: Overview): void =
    var 
        quartal = summary.quartal
        p2 = summary.p2
        p4 = summary.p4
        p31 = summary.p31
    echo fmt"{quartal:<10} {p2:>20.2f}€ (§2) {p4:>20.2f}€ (§4) {p31:>20.2f}€ (§31)".indent(2)


#prints an overview of a quarter
proc quarters(data: seq[Transaction]): void =
    var 
        groupedData: seq[tuple[k: string, v: seq[Transaction]]] = @[]
        overviews: seq[Overview] = @[]

    for i in groupBy(data, getQuarter):
        groupedData.add(i)
    for i in groupedData:
        overviews.add(getOverview(i[1]))
    for i in overviews:
        printOverview(i)
    
#prints amount of top payers/recipients
proc top(data: seq[Transaction],amount: int, por: string, paragraph: int): void =

    var 
        filtered: seq[Transaction] = @[]
        grouped: seq[tuple[k: string, v: seq[Transaction]]] = @[]
        tuples: seq[Pair] = @[]

    filtered = filterIt(data, it.bekannt == paragraph)
    
        
    if por == "payers":
        for i in groupBy(filtered, getRechtstraeger):
            grouped.add(i)

        tuples = grouped.mapIt(toPairwithSum(it))
        #for i in grouped:
        #    var foo = Pair(name: i[1][0].getRechtstraeger, euro: foldl(i[1].mapIt(it.euro), a + b))
        #    tuples.add(foo)
    else:
        for i in groupBy(filtered, getMedieninhaber):
            grouped.add(i)

        tuples = grouped.mapIt(toPairwithSum(it))
        #for i in grouped:
        #    var foo = Pair(name: i[1][0].getRechtstraeger, euro: foldl(i[1].mapIt(it.euro), a + b))
        #    tuples.add(foo)

    
    tuples.sort(proc (x,y: Pair): int = result = cmp(x.euro,y.euro), Descending)

    var 
        i = 0
        j = amount

    if j > len(tuples):
        j = len(tuples)

    while (i < j):
        var 
            foo = tuples[i]
            name = foo.name
            euro = foo.euro
        echo fmt"{name:<50} : {euro:<20.2f}".indent(2)
        i += 1

#prints all payers/recipients where searchterm is in rechtst/medienin
proc search(data: seq[Transaction], por: string, term: string): void =
    var res: seq[string]
    if (por == "payers"):
        res = data.filterIt(it.rechtst.toLowerAscii.contains(term.toLowerAscii)).mapIt(it.rechtst).deduplicate
    else:
        res = data.filterIt(it.medienin.toLowerAscii.contains(term.toLowerAscii)).mapIt(it.medienin).deduplicate
    var results = len(res)
    echo ($results & " results found:\n")
    for i in res:
        echo i.indent(2)
    echo ""

#prints a detailed view of a payer/recipient
proc details(data: seq[Transaction], por: string, name: string): void =
    var dis: seq[Transaction] = @[]
    if (por == "payers"):
        dis = data.filterIt(it.rechtst == name)
    else:
        dis = data.filterIt(it.medienin == name)
        #[
    var
        p2 = dis.filterIt(it.bekannt == 2).sorted(proc (x,y:Transaction):int = result = cmp(x.euro,y.euro),Descending)
        p4 = dis.filterIt(it.bekannt == 4).sorted(proc (x,y:Transaction):int = result = cmp(x.euro,y.euro),Descending)
        p31 = dis.filterIt(it.bekannt == 31).sorted(proc (x,y:Transaction):int = result = cmp(x.euro,y.euro),Descending)
    ]#
    var
        dis2 = dis.filterIt(it.bekannt == 2)
        dis4 = dis.filterIt(it.bekannt == 4)
        dis31 = dis.filterIt(it.bekannt == 31)
        p2: seq[tuple[k: string, v: seq[Transaction]]] = @[]
        p4: seq[tuple[k: string, v: seq[Transaction]]] = @[]
        p31: seq[tuple[k: string, v: seq[Transaction]]] = @[]

    if por == "payers":
        for x in groupBy(dis2, getMedieninhaber):
            p2.add(x)
        for x in groupBy(dis4, getMedieninhaber):
            p4.add(x)
        for x in groupBy(dis31, getMedieninhaber):
            p4.add(x)
    else:
        for x in groupBy(dis2, getRechtstraeger):
            p2.add(x)
        for x in groupBy(dis4, getRechtstraeger):
            p4.add(x)
        for x in groupBy(dis31, getRechtstraeger):
            p4.add(x)

    var    
        p2s = p2.mapIt(toPairwithSum(it))
        p4s = p4.mapIt(toPairwithSum(it))
        p31s = p31.mapIt(toPairwithSum(it))
    
    p2s.sort(proc (x,y: Pair): int = result = cmp(x.euro,y.euro),Descending)
    p4s.sort(proc (x,y: Pair): int = result = cmp(x.euro,y.euro),Descending)
    p31s.sort(proc (x,y: Pair): int = result = cmp(x.euro,y.euro),Descending)

    echo "Payments according to §2:".indent(2)
    for i in p2s:
        echo fmt"{i.name:<50} : {i.euro:>20.2f}".indent(4)
    echo "Payments according to §4:".indent(2)
    for i in p4s:
        echo fmt"{i.name:<50} : {i.euro:>20.2f}".indent(4)
    echo "Payments according to §31".indent(2)
    for i in p31s:
        echo fmt"{i.name:<50} : {i.euro:>20.2f}".indent(4)


#############################
#   Start of Main Program   #
#############################
var data: seq[Transaction] = @[]

if (paramCount() == 0):
    data = loadDataForQuarter("20191")
else:
    data = loadMultiple(commandLineParams())

proc main(): void =
    echo "Please enter a command or type \"help\" for more information"
    var input = readLine(stdin).split(" ")
    var command = input[0]
    var args: seq[string] = @[]
    if len(input) > 1:
        args = input[1 .. len(input)-1]
    case command
        of "help":
            printHelpMsg()
        of "exit":
            echo "Bye!"
            quit(QuitSuccess)
        of "quit":
            echo "Bye!"
            quit(QuitSuccess)
        of "payers":
            payers(data)
        of "recipients":
            recipients(data)
        of "load":
            data = loadMultiple(args)
        of "quarters":
            quarters(data)
        of "top":
            try:
                case args[1].toLowerAscii
                of "payers":
                    case args[2]
                        of "2":
                            top(data,parseInt(args[0]),"payers",2)
                        of "4":
                            top(data,parseInt(args[0]),"payers",4)
                        of "31":
                            top(data,parseInt(args[0]),"payers",31)
                        else:
                            echo "Wrong parameters for command 'top'"
                of "recipients":
                    case args[2]
                        of "2":
                            top(data,parseInt(args[0]),"recipients",2)
                        of "4":
                            top(data,parseInt(args[0]),"recipients",4)
                        of "31":
                            top(data,parseInt(args[0]),"recipients",31)
                        else:
                            echo "Wrong parameters for command 'top'"
                else:
                    echo "Wrong parameters for command 'top'"
            except IndexError:
                echo "Missing Parameters for command 'top'"
            except:
                let 
                    e = getCurrentException()
                    msg = getCurrentExceptionMsg()
                echo "Got exception " & repr(e) & " with message " & $msg
            finally:
                main()
        of "search":
            try:
                var term = args[1..len(args)-1].join(" ")
                case args[0]
                    of "payers":
                        search(data,"payers",term)
                    of "recipients":
                        search(data,"recipients",term)
                    else:
                        echo "Wrong parameters for command 'search'"
            except IndexError:
                echo "Missing Parameters for command 'search'"
            except RangeError:
                echo "Missing Parameters for command 'search'"
            except:
                let 
                    e = getCurrentException()
                    msg = getCurrentExceptionMsg()
                echo "Got exception " & repr(e) & " with message " & $msg
            finally:
                main()
        of "details":
            try:
                var name = args[1..len(args)-1].join(" ")
                case args[0]
                    of "payers":
                        details(data,"payers",name)
                    of "recipients":
                        details(data,"recipients",name)
                    else:
                        echo "Wrong parameters for command 'details'"
            except IndexError:
                echo "Missing Parameters for command 'details'"
            except RangeError:
                echo "Missing Parameters for command 'details'"
            except:
                let 
                    e = getCurrentException()
                    msg = getCurrentExceptionMsg()
                echo "Got exception " & repr(e) & " with message " & $msg
            finally:
                main()
        else:
            echo "Unknown command"
    main()

main()