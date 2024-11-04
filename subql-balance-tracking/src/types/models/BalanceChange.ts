// Auto-generated , DO NOT EDIT
import {Entity, FunctionPropertyNames, FieldsExpression, GetOptions } from "@subql/types-core";
import assert from 'assert';



export type BalanceChangeProps = Omit<BalanceChange, NonNullable<FunctionPropertyNames<BalanceChange>>| '_name'>;

export class BalanceChange implements Entity {

    constructor(
        
        id: string,
        balance: bigint,
        block_height: number,
        event: string,
        address: string,
    ) {
        this.id = id;
        this.balance = balance;
        this.block_height = block_height;
        this.event = event;
        this.address = address;
        
    }

    public id: string;
    public balance: bigint;
    public block_height: number;
    public timestamp?: Date;
    public event: string;
    public address: string;
    

    get _name(): string {
        return 'BalanceChange';
    }

    async save(): Promise<void>{
        let id = this.id;
        assert(id !== null, "Cannot save BalanceChange entity without an ID");
        await store.set('BalanceChange', id.toString(), this);
    }

    static async remove(id:string): Promise<void>{
        assert(id !== null, "Cannot remove BalanceChange entity without an ID");
        await store.remove('BalanceChange', id.toString());
    }

    static async get(id:string): Promise<BalanceChange | undefined>{
        assert((id !== null && id !== undefined), "Cannot get BalanceChange entity without an ID");
        const record = await store.get('BalanceChange', id.toString());
        if (record) {
            return this.create(record as BalanceChangeProps);
        } else {
            return;
        }
    }


    /**
     * Gets entities matching the specified filters and options.
     *
     * ⚠️ This function will first search cache data followed by DB data. Please consider this when using order and offset options.⚠️
     * */
    static async getByFields(filter: FieldsExpression<BalanceChangeProps>[], options?: GetOptions<BalanceChangeProps>): Promise<BalanceChange[]> {
        const records = await store.getByFields('BalanceChange', filter, options);
        return records.map(record => this.create(record as BalanceChangeProps));
    }

    static create(record: BalanceChangeProps): BalanceChange {
        assert(typeof record.id === 'string', "id must be provided");
        let entity = new this(
            record.id,
            record.balance,
            record.block_height,
            record.event,
            record.address,
        );
        Object.assign(entity,record);
        return entity;
    }
}
