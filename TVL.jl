# PACKAGE AREA

using DataFrames, CSV, VegaLite, Dates, JLD2, DataFramesMeta, Plots, StatsPlots, Tables, Statistics

# CONSTANT AREA - made for readability

const useTestEnvironment = false # change this to true/false when you want to run code on 1 dex file vs. all 10 dex files

const dexDatabasePath = "/home/DefiClass2022/databases/dexes/" # won't change for this project
const personalPath = "/home/markosch.saure/" # path used to for saving CSV files
const easyFile = "dexes_2022_10.jld2" # use this file for easy testing

const uniSwap = "B4E16D0168E52D35CACD2C6185B44281EC28C9DC" # log_emitter for Uniswap
const sushiSwap = "397FF1542F962076D0BFE58EA045FFA2D347ACA0" # log_emitter for SushiSwap
const dooarSwap = "9C2DC3D5FFCECF61312C5F4C00660695B32FB3D1" # log_emitter for dooarSwap
const topLiquidityPools = [uniSwap, sushiSwap, dooarSwap] # combining the log_emitters

const convRateETHToWETH = 1 / (1 * 10^18) # 1 ETH = 1 * 10^18 WETH (18 decimal places)
const convRateWETHToUSD = 1295.93 # 1 WETH = 1295.93 USD as of Oct 19 2022 1:35 AM MST
const convRateUSDCtoUSD = 1 / (1 * 10^6) # 1 USDC = 1 * 10^6 USD (6 decimal places)

const syncAddress = "1C411E9A96E071241C2F21F7726B17AE89E3CAB4C78BE50E062B03A9FFFBBAD1" # topic0 for sync events

#===============================================#

# HELPER FUNCTION AREA

# signed_at is the Date column but also includes the timestamp; this function splits the date and timestamp

function splitDate(myDate)
    dateParts = split(myDate, " ")
    return dateParts[1]
end

# data0 and data1 are strings that should be ints; this function converts strings to ints

function strToInt(string)
    if isequal(string, missing)
        return 0    
    else
        return parse(BigInt, string, base=16)
    end
end

#===============================================#

# ACTUAL CODE STARTS HERE

# empty dataframe to store all dex data; based off of previous Token project

dexDF = DataFrame(
    chain_id = Union{Missing,Int64}[],
    chain_name = String[],
    block_id = Union{Missing,Int64}[],
    block_hash = Union{Missing,String}[],
    signed_at = String[],
    block_parent_hash = Union{Missing,String}[],
    block_height = Union{Missing,Int64}[],
    block_miner = Union{Missing,String}[],
    block_mining_cost = Union{Missing,Int64}[],
    block_gas_limit = Union{Missing,Int64}[],
    block_gas_used = Union{Missing,Int64}[],
    tx_offset = Union{Missing,Int64}[],
    tx_hash = Union{Missing,String}[],
    successful = Union{Missing,Int64}[],
    tx_mining_cost = Union{Missing,Int64}[],
    tx_sender = Union{Missing,String}[],
    tx_recipient = Union{Missing,String}[],
    tx_creates = Union{Missing,String}[],
    tx_value = Union{Missing,Float64}[],
    tx_gas_offered = Union{Missing,Int64}[],
    tx_gas_spent = Union{Missing,Int64}[],
    tx_gas_price = Union{Missing,Int64}[],
    log_offset = Union{Missing,Int64}[],
    log_emitter = Union{Missing,String}[],
    topic0 = Union{Missing,String}[],
    topic1 = Union{Missing,String}[],
    topic2 = Union{Missing,String}[],
    topic3 = Union{Missing,String}[],
    data0 = Union{Missing,String}[],
    data1 = Union{Missing,String}[],
    data2 = Union{Missing,String}[],
    data3 = Union{Missing,String}[],
    fees_paid = String[]
)

# If testing, only load 1 file into the dex dataframe (for speed)
# Otherwise, iterate through all the dex files and append their data into the dex dataframe

if useTestEnvironment == true

    dexFile = load_object(dexDatabasePath * easyFile)
    global dexDF = vcat(dexDF, dexFile)

else  
    dexFiles = readdir(dexDatabasePath)[1:end-1]
    for f in dexFiles
        indDexFile = load_object(dexDatabasePath * f)
        global dexDF = vcat(dexDF, indDexFile)
    end

end 

# We will be analyzing the LP's with the most activity (i.e. most sync events aka UniSwap, SushiSwap, and DooarSwap)
# Note that this was determined from another set of code
# This code specifically will be graphing daily TVL (total value locked) for each pool

# Data clean-up needed for data0 and data1 (we will use these for TVL)
# data0 is in string hexadecimal in USDC, so we will need to do 2 things: convert hex to decimal (USDC) -> USDC to USD
# data1 is in string hexadecimal in ETH, so we will need to do 3 things: convert hex to decimal (ETH) -> ETH to WETH -> then WETH to USD

dexDF.:data0 = strToInt.(dexDF.:data0) * convRateUSDCtoUSD
dexDF.:data1 = strToInt.(dexDF.:data1) * convRateETHToWETH * convRateWETHToUSD

# TVL = data0 + data1

dexDF.:TVL = dexDF.:data0 + dexDF.:data1

# clean up the signed_at column (split date and timestamp; we only want date)

dexDF =  @rtransform dexDF $[:Date, :Time] = split(:signed_at)

# For each LP:

for LP in topLiquidityPools

    # Filter dataframe by that LP only (via log_emitter)

    liquidityPoolDF = dexDF[dexDF.:log_emitter .== LP, :]

    # Filter dataframe by sync events only (via topic0)

    syncOnlyDF = liquidityPoolDF[liquidityPoolDF.:topic0 .== syncAddress, :]

    # Concatenate TVL data by day

    dailyTVL = combine(groupby(syncOnlyDF, [:Date]), :TVL => sum) 

    # Export data as CSV to desired workspace

    CSV.write(personalPath * LP * ".csv", dailyTVL)

end
