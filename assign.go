package main

import (
	"fmt"
	"math"
	"sync"
	"bufio"
	"os"
	"strconv"
	"log"
	"strings"
)
type transport struct {
    filename       string
    supply, demand []int
    costs          [][]int
    table         [][]cell
}

type cell struct {
    weight, cost int
    row, col     int
}

type marcost struct {
	marCst int
	quantity int 
	path []cell
	startingPointOption cell
}

var nullCell = cell{}

var wg sync.WaitGroup

func check(err error) {
    if err != nil {
        log.Fatal(err)
    }
}

// this method reads the input and intial solution files
func readFile(filename, solutioFile string) *transport{

	file, err := os.Open(filename)
    check(err)
	

	// determining the length of demand and supply
	sc :=  bufio.NewScanner(file)
	sc.Split(bufio.ScanLines)
	
	sc.Scan()
	str := strings.Fields(sc.Text())
	demandsLength := len(str)-2
	x:=0
	for sc.Scan(){
		x++
	}
	supplyLength := x-1

	//intialising the table, supply and demand arrays
	table := make([][] cell, supplyLength)
	for i:=0; i<supplyLength; i++ {
		table[i] = make([]cell, demandsLength)
	}
	
	costs:= make([][] int, supplyLength)
	for i:=0; i<supplyLength; i++{
		costs[i] = make([]int, demandsLength)
	}

	supply:= make([] int, supplyLength)
	demand:= make([] int, demandsLength)
	file.Close()

	// reading the costs 
	file2, err := os.Open(filename)
	check(err)
	defer file2.Close()
	
	scanner := bufio.NewScanner(file2)
	scanner.Split(bufio.ScanLines)

	//skips the first line
	scanner.Scan()
	

	for i:=1; i<=supplyLength; i++ {
		scanner.Scan()
		source := strings.Fields(scanner.Text())
		
		for j:=1; j<= demandsLength; j++{
			cost,_:= strconv.Atoi(source[j]) 
			costs[i-1][j-1] = cost
		}
		supply[i-1],_ = strconv.Atoi(source[len(source)-1]) 
	}

	scanner.Scan()
	demSource := strings.Fields(scanner.Text())
	for i:=1; i<=demandsLength; i++ {
		demand[i-1],_ = strconv.Atoi(demSource[i])
	}

	// read the initial solution
	file3, err := os.Open(solutioFile)
	check(err)

	scSol :=  bufio.NewScanner(file3)
	scSol.Split(bufio.ScanLines)
	//skipping the first line

	scSol.Scan()
	for i:=1; i<=supplyLength; i++ {
		scSol.Scan()
		source := strings.Fields(scSol.Text())
		for j:=1; j<= demandsLength; j++{
			if source[j] != "-"{
				weight,_:= strconv.Atoi(source[j])
				table[i-1][j-1] = cell {weight , costs[i-1][j-1], i-1 , j-1}
			}
		}
		
	}

	// check for degenerate case
	ce:=0
	for i:=0; i<supplyLength; i++ {
		for j:=0; j< demandsLength; j++{
			if table[i][j] != nullCell{
				ce ++
			}
		}
	}

	if ce != demandsLength+supplyLength-1 {
		panic("degenerate case!")
	}

	return &transport{filename, supply, demand, costs, table}
}

//this method prints the current state of the table to the console
func (t *transport) printResult() {
	totalCosts := 0
    for row := 0; row < len(t.supply); row++ {
        for col := 0; col < len(t.demand); col++ {
            cell := t.table[row][col]
            if cell != nullCell {
                fmt.Printf(" %3d ", cell.weight)
                totalCosts += cell.weight * cell.cost
            } else {
                fmt.Printf("  -  ")
            }
        }
        fmt.Println()
    }
    fmt.Printf("\nTotal costs: %d\n\n", totalCosts)
}

// this method types the solution to a text file
func (t *transport) generateSolutionFile(filename string){
	f, _ := os.Create("Solution.txt")

	// copying the indexing row and column 
	file, err := os.Open(filename)
    check(err)
	defer file.Close()
	sc :=  bufio.NewScanner(file)
	sc.Split(bufio.ScanLines)
	
	sc.Scan()
	fmt.Fprintln(f, sc.Text())
	for i:=0; i<len(t.supply);i++ {
		sc.Scan()
	}
	
	

	totalCosts := 0
    for row := 0; row < len(t.supply); row++ {
		line := "Source"+strconv.Itoa(row+1)
        for col := 0; col < len(t.demand); col++ {
            cell := t.table[row][col]
            if cell != nullCell {
				s := fmt.Sprintf(" %3d ", cell.weight)
				line += s
                totalCosts += cell.weight * cell.cost
            } else {
				s:= fmt.Sprintf("  -  ")
				line += s
            }
		}
		line += strconv.Itoa(t.supply[row])
        fmt.Fprintln(f, line)
	}
	sc.Scan()
	fmt.Fprintln(f, sc.Text())
	tc:= fmt.Sprintf("\nTotal costs: %d\n\n", totalCosts)
    fmt.Fprintln(f,tc)

}

// Changes the non empty cells in the table to a 1D slice.
func (t *transport) tableTo1Dslice() []cell{
	var s[] cell
	for i:=0;i<len(t.supply);i++ {
		for j:=0; j<len(t.demand); j++ {
			if t.table [i][j] != nullCell {
				s = append(s,t.table[i][j])
			}
		}
	}
	return s
}

//Gets a closed a path for a given cell
func (t *transport) getPath(c cell) [] cell{
	 sli := t.tableTo1Dslice()
	 sli = append([]cell{c},sli...)

	//removing all the cells that does have cells in the same row or column.
	// keep doing this till the remaining cells are the path. 
	 for {
		 rem := 0
		 for i:=0; i<len(sli); i++ {
			 adjCells := t.adjacentCells(sli[i],sli)
			 if adjCells[0] == nullCell || adjCells[1] == nullCell {
				sli = append (sli[:i], sli[i+1:]...)
				rem++
			 }
		 }
		 if rem ==0 {
			 break
		 }
	 }
	
	 // orders cell in the slice in the correct plus minus order
	 pathElems := make([]cell, len(sli))
	 p := c
	 for i:=0; i<len(sli); i++ {
		 pathElems[i]=p
		 p = t.adjacentCells(p,sli)[i%2]
	 }
	return pathElems
}

//This method returns the first cell in the list that is in the same row as c
//and the first cell in the list that is in the same column as c.  
//if nothing exist, it returns empty cell
func (t *transport) adjacentCells(c cell, s[] cell) [2]cell{
	var adj [2]cell
	for i:=0; i<len(s); i++ {
		if s[i] != c {
			if s[i].row == c.row && adj[0] == nullCell  {
				adj[0] = s[i]
			} else if s[i].col == c.col && adj[1] == nullCell {
				adj[1] = s[i]
			}
			if adj[0] != nullCell && adj[1] != nullCell {
				break
			}
 		}
	}
	return adj
}

// This method creates the marginal cost of a given cell with a given path
//and find out the quantity to be moved of this path is to be used in optimisation.
//the results are send through a channel that is passed as an argument
func marginalCost(c cell, path[]cell, ch chan marcost){
	marginalcost :=0
	lowestQuantity := int(math.MaxInt32)
	var startingPointOption cell

	add:=true
	for i:=0; i<len(path); i++{
		if add {
			marginalcost += path[i].cost
		} else {
			marginalcost -= path[i].cost
			if path[i].weight < lowestQuantity {
				lowestQuantity = path[i].weight
				startingPointOption = path[i]
			}
		}
		add = !add
	}
	candidate := marcost{marginalcost, lowestQuantity, path, startingPointOption}
	ch <- candidate
	wg.Done()
}

func (t *transport) steppingStone(){
		var ch = make(chan marcost,10000)
		bestMarginalCost := 0
		var chosenPath []cell = nil
		startingPoint := nullCell

		
		for i:=0; i<len(t.supply); i++ {
			for j:=0; j<len(t.demand); j++ {
				//skips unempty cells
				if t.table[i][j] != nullCell {
					continue
				}
				// get closed path for this empty cell
				emptyCell := cell{0,t.costs[i][j],i , j}
				path := t.getPath(emptyCell)

				// retireve the marginal cost and quantity info via go routine
				wg.Add(1)
				go marginalCost(emptyCell, path, ch)
			}
		}
		wg.Wait()
		close(ch)
		
		// figure our the best path for a given state of the table
		for d := range ch {
			if d.marCst < bestMarginalCost {
				chosenPath = d.path
				startingPoint = d.startingPointOption
				bestMarginalCost = d.marCst
			}	
		}
		
		// if a valid path is found, start using it with the maximum allowed weight
		if len(chosenPath)!= 0 {
			w := startingPoint.weight
			add := true
			for _,c := range chosenPath {
				if add {
					c.weight += w
				} else {
					c.weight -= w
				}
				if c.weight == 0 {
					t.table[c.row][c.col] = nullCell
				}else {
					t.table[c.row][c.col] = c
				}
				add = !add
			}

			// keep doing the same thing till no more valid paths are found.
			// hence a solution is found
			t.steppingStone()
		}
}



func main(){
	var inputFile string
	var solutionFile string

	fmt.Print("Enter the name of the input file: ")
	fmt.Scanln(&inputFile)
	fmt.Print("Enter the name of the intial solution file: ")
	fmt.Scanln(&solutionFile)
	t:=readFile(inputFile, solutionFile)
	//t.printResult()
	t.steppingStone()
	//t.printResult()
	t.generateSolutionFile(inputFile)
}
